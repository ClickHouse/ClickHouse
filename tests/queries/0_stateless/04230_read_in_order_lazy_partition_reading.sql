-- Tags: no-random-merge-tree-settings, no-random-settings

-- Test lazy per-partition reading optimization for in-order reading.
-- With LIMIT, only the first partition(s) are actually read -> fewer read_rows.

SET optimize_read_in_order = 1;
SET read_in_order_allow_per_partition_lazy_read = 1;
SET read_in_order_use_virtual_row = 0;

-- ============================================================================
-- Table 1: Simple single-column ORDER BY (time).
-- No prefix columns to fix -> optimization always applies.
-- ============================================================================

DROP TABLE IF EXISTS t_lazy_simple;

CREATE TABLE t_lazy_simple (time DateTime, val UInt64)
ENGINE = MergeTree
PARTITION BY toYYYYMM(time)
ORDER BY time;

INSERT INTO t_lazy_simple
SELECT toDateTime('2024-01-01 00:00:00') + number * 60 AS time, number AS val FROM numbers(20000);
INSERT INTO t_lazy_simple
SELECT toDateTime('2024-02-01 00:00:00') + number * 60 AS time, number + 20000 AS val FROM numbers(20000);
INSERT INTO t_lazy_simple
SELECT toDateTime('2024-03-01 00:00:00') + number * 60 AS time, number + 40000 AS val FROM numbers(20000);
INSERT INTO t_lazy_simple
SELECT toDateTime('2024-04-01 00:00:00') + number * 60 AS time, number + 60000 AS val FROM numbers(20000);
INSERT INTO t_lazy_simple
SELECT toDateTime('2024-05-01 00:00:00') + number * 60 AS time, number + 80000 AS val FROM numbers(20000);
INSERT INTO t_lazy_simple
SELECT toDateTime('2024-06-01 00:00:00') + number * 60 AS time, number + 100000 AS val FROM numbers(20000);

OPTIMIZE TABLE t_lazy_simple FINAL;

SELECT 'parts:', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_lazy_simple' AND active;

-- ------------------------------------------------------------------
-- Test 1: ASC + LIMIT 5 -- fewer rows read with optimization enabled.
-- With optimization: reads only 1 partition (~20000 rows).
-- Without: reads initial granules from all 6 partitions.
-- ------------------------------------------------------------------
SELECT time, val
FROM t_lazy_simple
WHERE time >= '2024-01-01 00:00:00'
ORDER BY time ASC
LIMIT 5
SETTINGS log_comment = '04230_test_1_enabled'
FORMAT Null;

SELECT time, val
FROM t_lazy_simple
WHERE time >= '2024-01-01 00:00:00'
ORDER BY time ASC
LIMIT 5
SETTINGS read_in_order_allow_per_partition_lazy_read = 0, log_comment = '04230_test_1_disabled'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'test 1 asc reads less:', a.read_rows < b.read_rows
FROM
    (SELECT read_rows FROM system.query_log
     WHERE current_database = currentDatabase()
         AND log_comment = '04230_test_1_enabled'
         AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) a,
    (SELECT read_rows FROM system.query_log
     WHERE current_database = currentDatabase()
         AND log_comment = '04230_test_1_disabled'
         AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) b;

-- ------------------------------------------------------------------
-- Test 2: DESC + LIMIT
-- ------------------------------------------------------------------
SELECT time, val
FROM t_lazy_simple
WHERE time <= '2024-07-01 00:00:00'
ORDER BY time DESC
LIMIT 5
SETTINGS log_comment = '04230_test_2_enabled'
FORMAT Null;

SELECT time, val
FROM t_lazy_simple
WHERE time <= '2024-07-01 00:00:00'
ORDER BY time DESC
LIMIT 5
SETTINGS read_in_order_allow_per_partition_lazy_read = 0, log_comment = '04230_test_2_disabled'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'test 2 desc reads less:', a.read_rows < b.read_rows
FROM
    (SELECT read_rows FROM system.query_log
     WHERE current_database = currentDatabase()
         AND log_comment = '04230_test_2_enabled'
         AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) a,
    (SELECT read_rows FROM system.query_log
     WHERE current_database = currentDatabase()
         AND log_comment = '04230_test_2_disabled'
         AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) b;

-- ------------------------------------------------------------------
-- Test 3: No WHERE at all -- optimization still applies
-- ------------------------------------------------------------------
SELECT time, val
FROM t_lazy_simple
ORDER BY time ASC
LIMIT 5
SETTINGS log_comment = '04230_test_3_enabled'
FORMAT Null;

SELECT time, val
FROM t_lazy_simple
ORDER BY time ASC
LIMIT 5
SETTINGS read_in_order_allow_per_partition_lazy_read = 0, log_comment = '04230_test_3_disabled'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'test 3 no where reads less:', a.read_rows < b.read_rows
FROM
    (SELECT read_rows FROM system.query_log
     WHERE current_database = currentDatabase()
         AND log_comment = '04230_test_3_enabled'
         AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) a,
    (SELECT read_rows FROM system.query_log
     WHERE current_database = currentDatabase()
         AND log_comment = '04230_test_3_disabled'
         AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) b;

-- ------------------------------------------------------------------
-- Test 4: Larger LIMIT spanning 2 partitions -- still reads fewer rows
-- than without optimization.
-- With optimization: reads ~2 partitions (40000 rows).
-- Without optimization: reads initial granules from all 6 partitions.
-- ------------------------------------------------------------------
SELECT time, val
FROM t_lazy_simple
ORDER BY time ASC
LIMIT 25000
SETTINGS log_comment = '04230_test_4_enabled'
FORMAT Null;

SELECT time, val
FROM t_lazy_simple
ORDER BY time ASC
LIMIT 25000
SETTINGS read_in_order_allow_per_partition_lazy_read = 0, log_comment = '04230_test_4_disabled'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'test 4 two partitions reads less:', a.read_rows < b.read_rows
FROM
    (SELECT read_rows FROM system.query_log
     WHERE current_database = currentDatabase()
         AND log_comment = '04230_test_4_enabled'
         AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) a,
    (SELECT read_rows FROM system.query_log
     WHERE current_database = currentDatabase()
         AND log_comment = '04230_test_4_disabled'
         AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) b;

-- ------------------------------------------------------------------
-- Test 5: Correctness -- ASC and DESC results must be correct.
-- ------------------------------------------------------------------
SELECT 'test 5 correctness ASC:';
SELECT time, val FROM t_lazy_simple ORDER BY time ASC LIMIT 5;

SELECT 'test 5 correctness DESC:';
SELECT time, val FROM t_lazy_simple ORDER BY time DESC LIMIT 5;

DROP TABLE t_lazy_simple;

-- ============================================================================
-- Table 2: Composite ORDER BY (k1, k2, time).
-- Prefix columns k1, k2 must be fixed by WHERE for optimization to apply.
-- ============================================================================

DROP TABLE IF EXISTS t_lazy_composite;

CREATE TABLE t_lazy_composite (k1 String, k2 String, time DateTime, val UInt64)
ENGINE = MergeTree
PARTITION BY toYYYYMM(time)
ORDER BY (k1, k2, time);

INSERT INTO t_lazy_composite
SELECT 'aaa', 'bbb', toDateTime('2024-01-01 00:00:00') + number * 60, number FROM numbers(20000);
INSERT INTO t_lazy_composite
SELECT 'aaa', 'bbb', toDateTime('2024-02-01 00:00:00') + number * 60, number + 20000 FROM numbers(20000);
INSERT INTO t_lazy_composite
SELECT 'aaa', 'bbb', toDateTime('2024-03-01 00:00:00') + number * 60, number + 40000 FROM numbers(20000);
INSERT INTO t_lazy_composite
SELECT 'aaa', 'bbb', toDateTime('2024-04-01 00:00:00') + number * 60, number + 60000 FROM numbers(20000);
INSERT INTO t_lazy_composite
SELECT 'aaa', 'bbb', toDateTime('2024-05-01 00:00:00') + number * 60, number + 80000 FROM numbers(20000);
INSERT INTO t_lazy_composite
SELECT 'aaa', 'bbb', toDateTime('2024-06-01 00:00:00') + number * 60, number + 100000 FROM numbers(20000);

INSERT INTO t_lazy_composite
SELECT 'xxx', 'yyy', toDateTime('2024-01-01 00:00:00') + number * 60, number + 200000 FROM numbers(5000);
INSERT INTO t_lazy_composite
SELECT 'xxx', 'yyy', toDateTime('2024-02-01 00:00:00') + number * 60, number + 205000 FROM numbers(5000);


INSERT INTO t_lazy_composite
SELECT 'aaa', 'ccc', toDateTime('2024-01-01 00:00:00') + number * 60, number + 300000 FROM numbers(5000);
INSERT INTO t_lazy_composite
SELECT 'aaa', 'ccc', toDateTime('2024-02-01 00:00:00') + number * 60, number + 305000 FROM numbers(5000);

OPTIMIZE TABLE t_lazy_composite FINAL;

SELECT 'parts:', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_lazy_composite' AND active;

-- ------------------------------------------------------------------
-- Test 6: Both prefix columns fixed -- optimization SHOULD apply.
-- ------------------------------------------------------------------
SELECT k1, k2, time, val
FROM t_lazy_composite
WHERE k1 = 'aaa' AND k2 = 'bbb' AND time >= '2024-01-01 00:00:00'
ORDER BY k1, k2, time ASC
LIMIT 5
SETTINGS log_comment = '04230_test_6_enabled'
FORMAT Null;

SELECT k1, k2, time, val
FROM t_lazy_composite
WHERE k1 = 'aaa' AND k2 = 'bbb' AND time >= '2024-01-01 00:00:00'
ORDER BY k1, k2, time ASC
LIMIT 5
SETTINGS read_in_order_allow_per_partition_lazy_read = 0, log_comment = '04230_test_6_disabled'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'test 6 prefix fixed reads less:', a.read_rows < b.read_rows
FROM
    (SELECT read_rows FROM system.query_log
     WHERE current_database = currentDatabase()
         AND log_comment = '04230_test_6_enabled'
         AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) a,
    (SELECT read_rows FROM system.query_log
     WHERE current_database = currentDatabase()
         AND log_comment = '04230_test_6_disabled'
         AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) b;

-- ------------------------------------------------------------------
-- Test 7: Gap in prefix -- k1 fixed, k2 NOT fixed.
-- Optimization should NOT apply because different k2 values interleave
-- across partitions.
-- ------------------------------------------------------------------
SELECT k1, k2, time, val
FROM t_lazy_composite
WHERE k1 = 'aaa' AND time >= '2024-01-01 00:00:00'
ORDER BY k1, k2, time ASC
LIMIT 5
SETTINGS log_comment = '04230_test_7_enabled'
FORMAT Null;

SELECT k1, k2, time, val
FROM t_lazy_composite
WHERE k1 = 'aaa' AND time >= '2024-01-01 00:00:00'
ORDER BY k1, k2, time ASC
LIMIT 5
SETTINGS read_in_order_allow_per_partition_lazy_read = 0, log_comment = '04230_test_7_disabled'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- With gap in prefix, optimization is not applied, so read_rows should be equal.
SELECT 'test 7 gap same reads:', a.read_rows = b.read_rows
FROM
    (SELECT read_rows FROM system.query_log
     WHERE current_database = currentDatabase()
         AND log_comment = '04230_test_7_enabled'
         AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) a,
    (SELECT read_rows FROM system.query_log
     WHERE current_database = currentDatabase()
         AND log_comment = '04230_test_7_disabled'
         AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) b;

-- ------------------------------------------------------------------
-- Test 8: Correctness -- composite key results must be correct.
-- ------------------------------------------------------------------
SELECT 'test 8 correctness composite:';
SELECT k1, k2, time, val
FROM t_lazy_composite
WHERE k1 = 'aaa' AND k2 = 'bbb' AND time >= '2024-01-01 00:00:00'
ORDER BY k1, k2, time ASC
LIMIT 5;

-- ------------------------------------------------------------------
-- Test 9: Fixed columns are not added to ORDER BY - optimization SHOULD apply
-- ------------------------------------------------------------------
SELECT k1, k2, time, val
FROM t_lazy_composite
WHERE k1 = 'aaa' AND k2 = 'bbb' AND time >= '2024-01-01 00:00:00'
ORDER BY time ASC
LIMIT 5
SETTINGS log_comment = '04230_test_9_enabled'
FORMAT Null;

SELECT k1, k2, time, val
FROM t_lazy_composite
WHERE k1 = 'aaa' AND k2 = 'bbb' AND time >= '2024-01-01 00:00:00'
ORDER BY time ASC
LIMIT 5
SETTINGS read_in_order_allow_per_partition_lazy_read = 0, log_comment = '04230_test_9_disabled'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'test 9 redundant columns in order by:', a.read_rows < b.read_rows
FROM
    (SELECT read_rows FROM system.query_log
     WHERE current_database = currentDatabase()
         AND log_comment = '04230_test_9_enabled'
         AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) a,
    (SELECT read_rows FROM system.query_log
     WHERE current_database = currentDatabase()
         AND log_comment = '04230_test_9_disabled'
         AND type = 'QueryFinish'
     ORDER BY event_time_microseconds DESC LIMIT 1) b;

DROP TABLE t_lazy_composite;

-- ============================================================================
-- Table 3: PARTITION BY k ORDER BY k with integer k.
-- partition_id values are "10" and "2", so "10" < "2" lexicographically 
-- even though 10 > 2 numerically.
-- ============================================================================

DROP TABLE IF EXISTS t_lazy_int_partition;

CREATE TABLE t_lazy_int_partition (k Int32, val UInt64)
ENGINE = MergeTree
PARTITION BY k
ORDER BY k;

INSERT INTO t_lazy_int_partition VALUES (2, 200), (2, 201), (2, 202);
INSERT INTO t_lazy_int_partition VALUES (10, 1000), (10, 1001), (10, 1002);

OPTIMIZE TABLE t_lazy_int_partition FINAL;

-- ------------------------------------------------------------------
-- Test 10: Correctness ASC -- must return k=2 rows first.
-- ------------------------------------------------------------------
SELECT 'test 10 correctness integer partition ASC:';
SELECT k, val FROM t_lazy_int_partition ORDER BY (k, val) ASC LIMIT 3;

-- ------------------------------------------------------------------
-- Test 11: Correctness DESC -- must return k=10 rows first.
-- ------------------------------------------------------------------
SELECT 'test 11 correctness integer partition DESC:';
SELECT k, val FROM t_lazy_int_partition ORDER BY (k, val) DESC LIMIT 3;

DROP TABLE t_lazy_int_partition;

-- ============================================================================
-- Table 4: PARTITION BY k ORDER BY k with Nullable(Int32) k.
-- The optimization must be disabled for nullable partition keys.
-- ============================================================================

DROP TABLE IF EXISTS t_lazy_nullable_partition;

CREATE TABLE t_lazy_nullable_partition (k Nullable(Int32), val UInt64)
ENGINE = MergeTree
PARTITION BY k
ORDER BY k
SETTINGS allow_nullable_key = 1;

INSERT INTO t_lazy_nullable_partition VALUES (NULL, 999);
INSERT INTO t_lazy_nullable_partition VALUES (2, 200), (2, 201);
INSERT INTO t_lazy_nullable_partition VALUES (10, 1000), (10, 1001);

OPTIMIZE TABLE t_lazy_nullable_partition FINAL;

-- ------------------------------------------------------------------
-- Test 12: Correctness ASC NULLS LAST -- must return k=2 rows first,
-- not the NULL partition
-- ------------------------------------------------------------------
SELECT 'test 12 correctness nullable partition ASC:';
SELECT k, val FROM t_lazy_nullable_partition ORDER BY k ASC NULLS LAST, val ASC LIMIT 2;

-- ------------------------------------------------------------------
-- Test 13: Correctness DESC NULLS LAST -- must return k=10 rows first.
-- ------------------------------------------------------------------
SELECT 'test 13 correctness nullable partition DESC:';
SELECT k, val FROM t_lazy_nullable_partition ORDER BY k DESC NULLS LAST, val DESC LIMIT 2;

DROP TABLE t_lazy_nullable_partition;
