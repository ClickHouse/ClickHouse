-- Correctness of enable_group_by_top_k_optimization for signed integer keys.
-- Regression test: `shouldSkipNumeric` must dispatch on the column's `TypeIndex`, otherwise
-- negative keys are compared as large unsigned numbers (the hash key type is always `UInt64`).

-- Tags: no-parallel-replicas

-- CI profile sets max_rows_to_group_by, which disables the optimization; reset it.
SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;

SET enable_group_by_top_k_optimization = 1;

DROP TABLE IF EXISTS t_gbylimit_signed;

CREATE TABLE t_gbylimit_signed
(
    k_i8 Int8,
    k_i16 Int16,
    k_i32 Int32,
    k_i64 Int64,
    k_d32 Date32,
    val UInt64
) ENGINE = MergeTree ORDER BY k_i64;

-- The negative range is essential: without the fix, negative keys break ordering.
INSERT INTO t_gbylimit_signed
SELECT
    ((number % 200) - 100)::Int8,
    ((number % 10000) - 5000)::Int16,
    ((number * 7 + 13) % 40000 - 20000)::Int32,
    (number::Int64 - 25000),
    toDate32('2020-01-01') + INTERVAL ((number % 10000) - 5000) DAY,
    number
FROM numbers(50000);

SELECT 'int8_asc';
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'int8_desc';
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'int16_asc';
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'int16_desc';
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'int32_asc';
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'int32_desc';
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- Int64 ASC: the original failing case (hash table uses UInt64 for Int64).
SELECT 'int64_asc';
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'int64_desc';
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- Date32 ASC: signed 32-bit type stored as Int32.
SELECT 'date32_asc';
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'date32_desc';
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_gbylimit_signed;

-- Guard against the environment silently disabling the optimization.
SELECT 'optimization_applied_guard';
SELECT count() FROM (EXPLAIN actions = 1 SELECT number AS k FROM numbers(100) GROUP BY k ORDER BY k LIMIT 5) WHERE explain LIKE '%Top-K%';
