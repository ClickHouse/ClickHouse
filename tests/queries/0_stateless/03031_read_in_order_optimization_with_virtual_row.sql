
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

SYSTEM FLUSH LOGS;

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

SYSTEM FLUSH LOGS;

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

SYSTEM FLUSH LOGS;

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
read_in_order_use_buffering = false, --avoid buffer
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

SELECT '========';
-- from 02149_read_in_order_fixed_prefix
DROP TABLE IF EXISTS fixed_prefix;

CREATE TABLE fixed_prefix(a UInt32, b UInt32)
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 3;

SYSTEM STOP MERGES fixed_prefix;

INSERT INTO fixed_prefix VALUES (0, 100), (1, 2), (1, 3), (1, 4), (2, 5);
INSERT INTO fixed_prefix VALUES (0, 100), (1, 2), (1, 3), (1, 4), (2, 5);

SELECT a, b FROM fixed_prefix WHERE a = 1 ORDER BY b SETTINGS max_threads = 1;

DROP TABLE fixed_prefix;

SELECT '========';
-- currently don't support virtual row in this case
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

-- TODO: handle preliminary merge for this case, temporarily disable it
SET optimize_read_in_order = 0;
SELECT * FROM function_pk ORDER BY (A,-B) ASC limit 3 SETTINGS max_threads = 1;

DROP TABLE function_pk;
