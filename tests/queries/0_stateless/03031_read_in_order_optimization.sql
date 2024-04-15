
DROP TABLE IF EXISTS t;

CREATE TABLE t
(
    `x` UInt64,
    `y` UInt64,
    `z` UInt64,
    `k` UInt64
)
ENGINE = MergeTree
ORDER BY (x, y, z)
SETTINGS index_granularity = 8192;

INSERT INTO t SELECT
    number,
    number,
    number,
    number
FROM numbers(8192 * 3);

INSERT INTO t SELECT
    number + (8192 * 3),
    number + (8192 * 3),
    number + (8192 * 3),
    number + (8192 * 3)
FROM numbers(8192 * 3);

SELECT x
FROM t
ORDER BY x ASC
LIMIT 4
SETTINGS max_block_size = 8192,
read_in_order_two_level_merge_threshold = 0,
max_threads = 1,
optimize_read_in_order = 1;

SYSTEM FLUSH LOGS;

SELECT read_rows
FROM system.query_log
WHERE current_database = currentDatabase()
AND query like '%SELECT x%'
AND query not like '%system.query_log%'
ORDER BY query_start_time DESC, read_rows DESC
LIMIT 1;

DROP TABLE t;