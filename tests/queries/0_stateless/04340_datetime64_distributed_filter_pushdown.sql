-- Tags: no-parallel-replicas

DROP TABLE IF EXISTS t_04340_datetime64_distributed_filter_pushdown;

CREATE TABLE t_04340_datetime64_distributed_filter_pushdown
(
    device_id UInt32,
    data_item_id UInt32,
    data_time DateTime64(3, 'UTC'),
    data_value UInt64
)
ENGINE = MergeTree
ORDER BY (device_id, data_item_id, data_time);

INSERT INTO t_04340_datetime64_distributed_filter_pushdown VALUES
    (100, 1, fromUnixTimestamp64Milli(1697547086760), 3),
    (100, 1, fromUnixTimestamp64Milli(1697547086761), 4),
    (100, 1, fromUnixTimestamp64Milli(1697547086762), 5),
    (100, 1, fromUnixTimestamp64Milli(1697547086763), 6);

SET enable_analyzer = 1;
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET prefer_localhost_replica = 1;
SET serialize_query_plan = 0;

SELECT
    count(),
    sum(max_data_value)
FROM
(
    SELECT
        device_id,
        data_item_id,
        data_time,
        max(data_value) AS max_data_value
    FROM remote('127.0.0.{1,2}', currentDatabase(), t_04340_datetime64_distributed_filter_pushdown)
    GROUP BY
        device_id,
        data_item_id,
        data_time
)
WHERE data_time >= fromUnixTimestamp64Milli(0, 'UTC');

DROP TABLE t_04340_datetime64_distributed_filter_pushdown;
