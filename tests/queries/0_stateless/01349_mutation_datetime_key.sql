DROP TABLE IF EXISTS cdp_orders;

CREATE TABLE cdp_orders
(
    `order_id` String,
    `order_status` String,
    `order_time` DateTime
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(order_time)
ORDER BY (order_time, order_id)
SETTINGS index_granularity = 8192;

INSERT INTO cdp_orders VALUES ('hello', 'world', '2020-01-02 03:04:05');

SELECT * FROM cdp_orders;
SET mutations_sync = 1;
ALTER TABLE cdp_orders DELETE WHERE order_time >= '2019-12-03 00:00:00';
SELECT * FROM cdp_orders;

DROP TABLE cdp_orders;
