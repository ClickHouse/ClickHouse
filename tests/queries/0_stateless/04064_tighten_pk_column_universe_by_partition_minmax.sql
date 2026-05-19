CREATE TABLE test_order_by (
    event_time DateTime,
    tenant_id UInt32,
    id UInt32
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (tenant_id, id, event_time)
SETTINGS
    index_granularity = 512,
    primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 1;

INSERT INTO test_order_by
SELECT
    toDateTime('2024-01-01 00:00:00') + toIntervalHour(number % 240) AS event_time,
    0 AS tenant_id,
    cityHash64(number) % 5000 AS id
FROM numbers(1_000_000);

OPTIMIZE TABLE test_order_by FINAL;

EXPLAIN ESTIMATE
SELECT *
FROM test_order_by
WHERE
   ((id = 100) AND ((event_time >= toDateTime('2024-01-01 23:00:00')) AND (event_time <= toDateTime('2024-01-03 01:00:00'))))
OR ((id = 200) AND ((event_time >= toDateTime('2024-01-04 23:00:00')) AND (event_time <= toDateTime('2024-02-06 01:00:00'))))
OR ((id = 300) AND ((event_time >= toDateTime('2024-01-07 23:00:00')) AND (event_time <= toDateTime('2024-03-09 01:00:00'))))
OR ((id = 400) AND ((event_time >= toDateTime('2024-01-10 23:00:00')) AND (event_time <= toDateTime('2024-04-12 01:00:00'))))
OR ((id = 500) AND ((event_time >= toDateTime('2024-01-13 23:00:00')) AND (event_time <= toDateTime('2024-05-15 01:00:00'))))
OR ((id = 600) AND ((event_time >= toDateTime('2024-01-15 23:00:00')) AND (event_time <= toDateTime('2024-06-18 01:00:00'))));

DROP TABLE test_order_by;

CREATE OR REPLACE TABLE test_order_by_skip
(
    `event_time` UInt32,
    `id` UInt32
)
ENGINE = MergeTree
PARTITION BY intDiv(event_time, 5000)
ORDER BY (id, event_time)
SETTINGS index_granularity = 512, primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.1;

INSERT INTO test_order_by_skip                                                                                     
SELECT
    number AS event_time, 
    intDiv(number, 1000) AS id
FROM numbers(10000);

EXPLAIN ESTIMATE
SELECT *
FROM test_order_by_skip
WHERE ((id = 0) AND (event_time = 1)) OR ((id = 2) AND (event_time > 5000));

DROP TABLE test_order_by_skip;
