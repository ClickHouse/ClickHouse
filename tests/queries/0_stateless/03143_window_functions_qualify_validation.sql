DROP TABLE IF EXISTS uk_price_paid;
CREATE TABLE uk_price_paid
(
    `price` UInt32,
    `date` Date,
    `postcode1` LowCardinality(String),
    `postcode2` LowCardinality(String),
    `type` Enum8('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
    `is_new` UInt8,
    `duration` Enum8('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
    `addr1` String,
    `addr2` String,
    `street` LowCardinality(String),
    `locality` LowCardinality(String),
    `town` LowCardinality(String),
    `district` LowCardinality(String),
    `county` LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2);

SELECT count(), (quantile(0.9)(price) OVER ()) AS price_quantile FROM uk_price_paid WHERE toYear(date) = 2023 QUALIFY price > price_quantile; -- { serverError NOT_AN_AGGREGATE }

SELECT count() FROM uk_price_paid WHERE toYear(date) = 2023 QUALIFY price > (quantile(0.9)(price) OVER ()); -- { serverError NOT_AN_AGGREGATE }

DROP TABLE uk_price_paid;
