-- https://github.com/ClickHouse/ClickHouse/issues/44668

CREATE TABLE temp
(
    `id` UInt64,
    `field1` UUID,
    `field2` UUID,
    `field3` Int64,
    `field4` Int64,
    `field5` LowCardinality(String),
    `field6` FixedString(3),
    `field7` String,
    `field8` Nullable(UUID),
    `event_at` DateTime('UTC'),
    `order_id` Nullable(UUID),
    `identity` LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_at)
ORDER BY (field1, event_at, field2, field5, id)
SETTINGS index_granularity = 8192;

INSERT INTO temp (id, field1, field2, field3, field4, field5, field6, field7, field8, event_at, order_id, identity)
VALUES ('1011','1d83904a-c31d-4a6c-bbf0-217656b46444','1d83904a-c31d-4a6c-bbf0-217656b46444',-200,0,'FOO','BAR','','1d83904a-c31d-4a6c-bbf0-217656b46444','2022-12-18 03:14:56','','dispatcher'),('10112222334444','1d83904a-c31d-4a6c-bbf0-217656b46444','1d83904a-c31d-4a6c-bbf0-217656b46444',12300,0,'FOO','BAR','','1d83904a-c31d-4a6c-bbf0-217656b46444','2022-12-17 23:37:18','1d83904a-c31d-4a6c-bbf0-217656b46444','other');

SELECT * FROM temp ORDER BY id;
