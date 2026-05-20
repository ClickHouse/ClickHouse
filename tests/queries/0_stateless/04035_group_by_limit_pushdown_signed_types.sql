-- Tests for correctness of the enable_group_by_top_k_optimization optimization.
-- Regression test: signed integer key types with the typed numeric fast path.
--
-- The hash table maps all 8-byte keys through `HashMethodOneNumber<UInt64>`,
-- so the typed fast path in `shouldSkipNumeric` receives `UInt64` as the
-- hash key type even when the actual column is `Int64`. Without proper
-- type dispatch based on the column's `TypeIndex`, negative values would be
-- compared as large unsigned numbers, producing wrong results.
--
-- Covers Int8, Int16, Int32, Int64 keys with both ASC and DESC ordering,
-- as well as Date32 (a signed 32-bit type stored in a `ColumnVector<Int32>`).

-- Tags: no-parallel-replicas

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

-- Insert data that includes negative values for all signed types.
-- The negative range is essential: without the fix, negative keys
-- are treated as huge positive unsigned values, breaking ordering.
INSERT INTO t_gbylimit_signed
SELECT
    ((number % 200) - 100)::Int8,
    ((number % 10000) - 5000)::Int16,
    ((number * 7 + 13) % 40000 - 20000)::Int32,
    (number::Int64 - 25000),
    toDate32('2020-01-01') + INTERVAL ((number % 10000) - 5000) DAY,
    number
FROM numbers(50000);

-- =====================
-- Int8 ASC
-- =====================
SELECT 'int8_asc';
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Int8 DESC
-- =====================
SELECT 'int8_desc';
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Int16 ASC
-- =====================
SELECT 'int16_asc';
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Int16 DESC
-- =====================
SELECT 'int16_desc';
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Int32 ASC
-- =====================
SELECT 'int32_asc';
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Int32 DESC
-- =====================
SELECT 'int32_desc';
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Int64 ASC (the original failing case: hash table uses UInt64 for Int64)
-- =====================
SELECT 'int64_asc';
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Int64 DESC
-- =====================
SELECT 'int64_desc';
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Date32 ASC (signed 32-bit type stored as Int32)
-- =====================
SELECT 'date32_asc';
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Date32 DESC
-- =====================
SELECT 'date32_desc';
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_gbylimit_signed;
