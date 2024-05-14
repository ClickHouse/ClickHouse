
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
SETTINGS index_granularity = 8192,
index_granularity_bytes = 10485760;

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
    number
FROM numbers(8192 * 3);

SYSTEM STOP MERGES t;

-- Expecting 2 virtual rows + one chunk (8192) for result + one extra chunk for next consumption in merge transform (8192),
-- both chunks come from the same part.
SELECT x
FROM t
ORDER BY x ASC
LIMIT 4
SETTINGS max_block_size = 8192,
read_in_order_two_level_merge_threshold = 0,  --force preliminary merge
max_threads = 1,
optimize_read_in_order = 1,
log_comment = 'preliminary merge, no filter';

SYSTEM FLUSH LOGS;

SELECT read_rows
FROM system.query_log
WHERE current_database = currentDatabase()
AND log_comment = 'preliminary merge, no filter'
AND type = 'QueryFinish'
ORDER BY query_start_time DESC 
limit 1;

-- Expecting 2 virtual rows + two chunks (8192*2) get filtered out + one chunk for result (8192),
-- all chunks come from the same part.
SELECT k
FROM t
WHERE k > 8192 * 2
ORDER BY x ASC
LIMIT 4
SETTINGS max_block_size = 8192,
read_in_order_two_level_merge_threshold = 0,  --force preliminary merge
max_threads = 1,
optimize_read_in_order = 1,
log_comment = 'preliminary merge with filter';

SYSTEM FLUSH LOGS;

SELECT read_rows
FROM system.query_log
WHERE current_database = currentDatabase()
AND log_comment = 'preliminary merge with filter'
AND type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 1;

-- Expecting 2 virtual rows + one chunk (8192) for result + one extra chunk for next consumption in merge transform (8192),
-- both chunks come from the same part.
SELECT x
FROM t
ORDER BY x ASC
LIMIT 4
SETTINGS max_block_size = 8192,
read_in_order_two_level_merge_threshold = 5, --avoid preliminary merge
max_threads = 1,
optimize_read_in_order = 1,
log_comment = 'no preliminary merge, no filter';

SYSTEM FLUSH LOGS;

SELECT read_rows
FROM system.query_log
WHERE current_database = currentDatabase()
AND log_comment = 'no preliminary merge, no filter'
AND type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 1;

-- Expecting 2 virtual rows + two chunks (8192*2) get filtered out + one chunk for result (8192),
-- all chunks come from the same part.
SELECT k
FROM t
WHERE k > 8192 * 2
ORDER BY x ASC
LIMIT 4
SETTINGS max_block_size = 8192,
read_in_order_two_level_merge_threshold = 5, --avoid preliminary merge
max_threads = 1,
optimize_read_in_order = 1,
log_comment = 'no preliminary merge, with filter';

SYSTEM FLUSH LOGS;

SELECT read_rows
FROM system.query_log
WHERE current_database = currentDatabase()
AND log_comment = 'no preliminary merge, with filter'
AND type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 1;

DROP TABLE t;