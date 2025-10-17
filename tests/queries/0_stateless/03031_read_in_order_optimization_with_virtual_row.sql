-- Tags: no-parallel-replicas
-- ^ because we are using query_log

SET read_in_order_use_virtual_row = 1;
SET use_query_condition_cache = 0;

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

SYSTEM STOP MERGES t;

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

SYSTEM FLUSH LOGS query_log;

SELECT read_rows
FROM system.query_log
WHERE current_database = currentDatabase()
AND log_comment = 'preliminary merge, no filter'
AND type = 'QueryFinish'
ORDER BY query_start_time DESC
limit 1;

SELECT '========';
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

SYSTEM FLUSH LOGS query_log;

SELECT read_rows
FROM system.query_log
WHERE current_database = currentDatabase()
AND log_comment = 'preliminary merge with filter'
AND type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 1;

SELECT '========';
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

SYSTEM FLUSH LOGS query_log;

SELECT read_rows
FROM system.query_log
WHERE current_database = currentDatabase()
AND log_comment = 'no preliminary merge, no filter'
AND type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 1;

SELECT '========';
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

SYSTEM FLUSH LOGS query_log;

SELECT read_rows
FROM system.query_log
WHERE current_database = currentDatabase()
AND log_comment = 'no preliminary merge, with filter'
AND type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 1;

DROP TABLE t;

SELECT '========';
-- from 02149_read_in_order_fixed_prefix
DROP TABLE IF EXISTS fixed_prefix;

CREATE TABLE fixed_prefix(a UInt32, b UInt32)
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 3;

SYSTEM STOP MERGES fixed_prefix;

INSERT INTO fixed_prefix VALUES (0, 100), (1, 2), (1, 3), (1, 4), (2, 5);
INSERT INTO fixed_prefix VALUES (0, 100), (1, 2), (1, 3), (1, 4), (2, 5);

SELECT a, b
FROM fixed_prefix
WHERE a = 1
ORDER BY b
SETTINGS max_threads = 1,
optimize_read_in_order = 1,
read_in_order_two_level_merge_threshold = 0;  --force preliminary merge

SELECT a, b
FROM fixed_prefix
WHERE a = 1
ORDER BY b
SETTINGS max_threads = 1,
optimize_read_in_order = 1,
read_in_order_two_level_merge_threshold = 5;  --avoid preliminary merge

DROP TABLE fixed_prefix;

SELECT '========';
DROP TABLE IF EXISTS function_pk;

CREATE TABLE function_pk
(
    `A` Int64,
    `B` Int64
)
ENGINE = MergeTree ORDER BY (A, -B)
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES function_pk;

INSERT INTO function_pk values(1,1);
INSERT INTO function_pk values(1,3);
INSERT INTO function_pk values(1,2);

SELECT *
FROM function_pk
ORDER BY (A,-B) ASC
limit 3
SETTINGS max_threads = 1,
optimize_read_in_order = 1,
read_in_order_two_level_merge_threshold = 5;  --avoid preliminary merge

DROP TABLE function_pk;

-- modified from 02317_distinct_in_order_optimization
SELECT '-- test distinct ----';

DROP TABLE IF EXISTS distinct_in_order SYNC;

CREATE TABLE distinct_in_order
(
    `a` int,
    `b` int,
    `c` int
)
ENGINE = MergeTree
ORDER BY (a, b)
SETTINGS index_granularity = 8192,
index_granularity_bytes = '10Mi';

SYSTEM STOP MERGES distinct_in_order;

INSERT INTO distinct_in_order SELECT
    number % number,
    number % 5,
    number % 10
FROM numbers(1, 1000000);

SELECT DISTINCT a
FROM distinct_in_order
ORDER BY a ASC
SETTINGS read_in_order_two_level_merge_threshold = 0,
optimize_read_in_order = 1,
max_threads = 2;

DROP TABLE distinct_in_order;
