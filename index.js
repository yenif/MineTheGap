const AWS = require('aws-sdk');
const Big = require("big.js");
const Gdax = require("coinbase-pro");

let gdaxClient;
function getGdaxClient() {
    if (typeof gdaxClient !== "undefined") {
        return gdaxClient;
    }

    return gdaxClient = Promise.all([
        decryptEnv("GDAX_KEY"),
        decryptEnv("GDAX_SECRET"),
        decryptEnv("GDAX_PASSPHRASE")
    ]).then(([key, secret, passphrase]) => {
        return new Gdax.AuthenticatedClient(
            key || process.env["UNSAFE_KEY"],
            secret || process.env["UNSAFE_SECRET"],
            passphrase || process.env["UNSAFE_PASSPHRASE"],
            process.env["GDAX_URL"] || "https://api-public.sandbox.pro.coinbase.com"
        );
    });
}

let kms;
const envDecrypted = {};
function decryptEnv(envKey) {
    if (envDecrypted.hasOwnProperty(envKey)) {
        return envDecrypted[envKey];
    }

    let encrypted = process.env[envKey];

    if (process.env["NODE_ENV"] === "dev") {
        return envDecrypted[envKey] = Promise.resolve(encrypted);
    }

    if (typeof encrypted === "undefined") {
        return envDecrypted[envKey] = Promise.resolve(null);
    }

    if (typeof kms === "undefined") {
        kms = new AWS.KMS();
    }

    return envDecrypted[envKey] = new Promise(
        (resolve, reject) => {
            // Decrypt code should run once and variables stored outside of the function
            // handler so that these are decrypted once per container
            kms.decrypt({
                CiphertextBlob: Buffer.from(encrypted, 'base64'),
                EncryptionContext: { LambdaFunctionName: process.env.AWS_LAMBDA_FUNCTION_NAME }
            }, (err, data) => {
                if (err) {
                    console.error("Failed on env key: " + envKey + " : " + encrypted)
                    return reject(err);
                }
                resolve(data.Plaintext.toString('ascii'));
            })
        }
    );
}

function processMarket(balanceExchange, countTradeBrackets, tradeIncrement, minIncrementRatio) {
    return Promise.all([
        getAccounts(),
        getProduct(balanceExchange),
        getProductOrderBook(balanceExchange),
        getProductOrders(balanceExchange),
    ]).then(
        ([accounts, product, book, orders]) => {
            let bid = Big(book.bids[0][0]);
            let ask = Big(book.asks[0][0]);

            let base = accounts.find(a => a.currency === product.base_currency);
            let quote = accounts.find(a => a.currency === product.quote_currency);

            return {
                bid,
                ask,
                base,
                quote,
                product,
                orders,
                countTradeBrackets,
                tradeIncrement
            };
        }
    ).then((market) => {
        // start in the middle (bid + ask) / 2
        let price = market.bid.plus(market.ask).div(2);

        // truncate to tradeIncrement
        market.avgPrice = price = price.minus(price.mod(tradeIncrement));

        let sellLimits = Array(Math.floor(countTradeBrackets / 2)).fill(0).map(
            (_, i) => {
                return price.plus(tradeIncrement.times(
                    (i + 1) // Increment to account for truncation
                ));
            }
        );
        let buyLimits = Array(Math.floor(countTradeBrackets / 2)).fill(0).map(
            (_, i) => {
                return price.minus(tradeIncrement.times(i));
            }
        );

        let buyOrders = balanceOrderForPrices(buyLimits, market, minIncrementRatio);
        let sellOrders = balanceOrderForPrices(sellLimits, market, minIncrementRatio);

        let limitOrders = buyOrders.concat(sellOrders).filter(({order}) => {
            switch(order.side) {
                case "buy":
                    // I'm buying, bid lower than ask
                    return order.price.lt(market.ask);
                case "sell":
                    // I'm selling, ask more than bid
                    return order.price.gt(market.bid);
                default:
                    return false;
            }
        }).filter((order) => {
            // Don't drop below 1 BTC or ETH
            return order.quoteBalanceAfter.gt(0.9) && order.baseBalanceAfter.gt(0.9);
        });

        let balanceOrders = {};
        let balance = balanceOrderForPrice(market.ask, market);
        if(balance.order.side === "sell") {
            balanceOrders["sell"] = balance;
        }

        balance = balanceOrderForPrice(market.bid, market);
        if(balance.order.side === "buy") {
            balanceOrders["buy"] = balance;
        }

        return { balanceOrders, limitOrders, market };
    });
}

function balanceOrderForPrices(prices, market, minIncrementRatio) {
    let order = {
        "baseBalanceAfter": Big(market.base.balance),
        "quoteBalanceAfter": Big(market.quote.balance)
    };
    return prices.map(
        (price) => {
            let newOrder = balanceOrderForPrice(price, market, order);

            if (newOrder.order.size.gte(market.product.base_min_size) && newOrder.priceRatio.gt(minIncrementRatio)) {
                order = newOrder;
            }

            return newOrder;
        }
    );
}

function balanceOrderForPrice(price, {product, quote, base}, priorOrder = {}) {
    price = Big(price).minus(price.mod(product.quote_increment));

    const quoteBalance = priorOrder.quoteBalanceAfter || Big(quote.balance);
    const baseBalance = priorOrder.baseBalanceAfter || Big(base.balance);
    const baseInQuoteBalance = baseBalance.times(price);
    const quoteDifference = quoteBalance.minus(baseInQuoteBalance);

    const lastPrice = (priorOrder.order||{}).price || quoteBalance.div(baseBalance);
    const priceRatio = price.minus(lastPrice).abs().div(lastPrice)

    const side = quoteDifference.s > 0 ? "buy" : "sell";
    const size = quoteDifference.div(2).div(price).abs();
    const product_id = product.id;

    const quoteBalanceAfter = quoteBalance.minus(quoteDifference.div(2));
    const baseBalanceAfter = baseInQuoteBalance.plus(
        quoteDifference.div(2)
    ).div(price);

    return {
        "order": {
            side,
            price,
            size,
            product_id
        },
        lastPrice,
        priceRatio,
        "quoteCurrency": quote.currency,
        quoteBalance,
        quoteDifference,
        quoteBalanceAfter,
        "quoteChange": quoteBalance.div(quoteBalanceAfter),
        baseBalance,
        baseInQuoteBalance,
        baseBalanceAfter,
        "baseChange": baseBalance.div(baseBalanceAfter)
    };
}

function getAccounts() {
    return getGdaxClient().then((authClient) => {
        return authClient.getAccounts();
    });
}

function getProduct(exc) {
    return getGdaxClient().then((authClient) => {
        return authClient.getProducts();
    }).then((products) => {
        return products.find(p => p.id === exc);
    });
}

function getProductOrderBook(exc) {
    return getGdaxClient().then((authClient) => {
        let result = authClient.getProductOrderBook(exc);
        return result;
    });
}

function getProductOrders(exc) {
    return getGdaxClient().then((authClient) => {
        return authClient.getOrders();
    }).then((orders) => {
        return orders.filter(
            order => { return order.product_id === exc; }
        );
    });
}

function getProductFills(exc) {
    return getGdaxClient().then((authClient) => {
        return authClient.getFills();
    }).then((fills) => {
        return fills.filter(
            fill => { return fill.product_id === exc; }
        );
    });
}

function buildOrders(tradeIncrement, maxSize, minIncrementRatio, {balanceOrders, limitOrders, market}) {
    const realOrders = [];
    const remove_oldOrders = [];

    var maxLimit = false;
    var minLimit = false;
    for (const i in limitOrders) {
        let realOrder = Object.assign({
            "remove_oldOrders": []
        }, limitOrders[i].order);

        for (const j in market.orders) {
            let order = market.orders[j];

            if (realOrder.price.eq(order.price)) {
                if (
                    realOrder.side === order.side &&
                    realOrder.size.gte(order.size) &&
                    limitOrders[i].priceRatio.gt(minIncrementRatio)
                ) {
                    realOrder.size = realOrder.size.minus(order.size);
                } else {
                    realOrder.remove_oldOrders = realOrder.remove_oldOrders.concat([order]);
                }
            }
        }

        if (realOrder.size.gt(maxSize)) {
            realOrder.size = maxSize;
        }

        // toFixed rounds, I want to truncate at 10e-8
        realOrder.size = realOrder.size.minus(
            realOrder.size.mod(Big(10).pow(-8))
        ).toFixed(8);

        maxLimit = maxLimit && maxLimit.gt(realOrder.price) ? maxLimit : realOrder.price;
        minLimit = minLimit && minLimit.lt(realOrder.price) ? minLimit : realOrder.price;

        if (
            Big(realOrder.size).gt(market.product.base_min_size) &&
            limitOrders[i].priceRatio.gt(minIncrementRatio)
        ) {
            realOrders.push({
                "side": realOrder.side,
                "price": realOrder.price.toString(),
                "size": realOrder.size,
                "product_id": realOrder.product_id,
                "remove_oldOrders": realOrder.remove_oldOrders,
                "post_only": true
            });
        } else if(realOrder.remove_oldOrders.length > 0) {
            realOrders.push({
                "remove_oldOrders": realOrder.remove_oldOrders
            });
        }
    }

    for (const j in market.orders) {
        let order = market.orders[j];

        if (minLimit.gt(order.price) || maxLimit.lt(order.price)) {
            continue;
        }

        if (Big(order.price).mod(tradeIncrement).gt(0)){
            remove_oldOrders.append(order);
        }
    }

    if (remove_oldOrders.length > 0) {
        realOrders.unshift({
            "remove_oldOrders": remove_oldOrders
        });
    }

    return {
        realOrders, balanceOrders, limitOrders, market
    };
}

let globalOrderIndex = 0;
function executeOrders({realOrders, balanceOrders, limitOrders, market}) {
    return getGdaxClient().then((authClient) => {
        return Promise.all(realOrders.map((order) => {
            let orderIndex = ++globalOrderIndex;
            let removes = order.remove_oldOrders.map((oldOrder) => {
                console.info(`${orderIndex} Cancel order ${oldOrder.id}`);
                return authClient.cancelOrder(oldOrder.id);
            });
            delete order.remove_oldOrders;

            return Promise.all(removes).then((removes_result) => {
                if(order.size === undefined) {
                    return removes_result;
                }

                console.info(
                    `${orderIndex} Place order ${order.side} ${order.size} at ${order.price} = ${order.size * order.price}`
                );
                return authClient.placeOrder(order).then((result) => {
                    console.info(
                        `${orderIndex} Accepted order ${result.id}`
                    );
                    order.orderIndex = result.orderIndex = orderIndex;
                    return removes_result.concat([result]);
                });
            });
        }));
    }).then((receipts) => {
        return {
            receipts, realOrders, balanceOrders, limitOrders, market
        };
    });
}

exports.handler = (event, context, callback) => {
    let product = event.product || "BTC-USD";
    let countTradeBrackets = event.countTradeBrackets || 6;
    let tradeIncrement = Big(event.tradeIncrement || ".001");
    let minIncrementRatio = Big(event.minIncrementRatio || ".02")
    let maxSize = Big(event.maxSize || 10000);

    return processMarket(
        product,
        countTradeBrackets,
        tradeIncrement,
        minIncrementRatio
    ).then(
        curry(buildOrders, tradeIncrement, maxSize, minIncrementRatio)
    ).then(
        executeOrders
    ).then(
        (result) => {
            console.log(JSON.stringify(result));
            callback(null, result);
        }
    ).catch(
        (err) => {
            console.error(err);
            callback(err);
        }
    );
};

exports.Feed = {
    balanceOrderForPrice
}

// Utils
function curry(fn, ...args) {
    return (...callingArgs) => {
        return fn(...(args.concat(callingArgs)));
    };
}
