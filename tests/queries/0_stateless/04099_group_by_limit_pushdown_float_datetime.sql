-- Tests for correctness of the enable_group_by_top_k_optimization optimization.
-- Regression test: Float32/Float64 and DateTime key types with the typed
-- numeric fast path in `TopKAggregationHeap`.
--
-- Float keys exercise `CompareHelper<Float32/64>` (via `FloatCompareHelper`),
-- which handles NaN specially: NaN is treated like NULL and sorted according
-- to the `nulls_direction` parameter.  Without correct NaN handling the heap
-- comparison would produce wrong ordering.
--
-- DateTime keys exercise the `UInt32` fast path instantiation under the
-- `TypeIndex::DateTime` case, verifying that the epoch-second representation
-- sorts correctly.

-- Tags: no-parallel-replicas

SET enable_group_by_top_k_optimization = 1;

-- ===================================================================
-- Float32 / Float64 tests (including NaN)
-- ===================================================================

DROP TABLE IF EXISTS t_gbylimit_float;

CREATE TABLE t_gbylimit_float
(
    k_f32 Float32,
    k_f64 Float64,
    val UInt64
) ENGINE = MergeTree ORDER BY val;

-- Generate ~50000 rows with a mix of negative, positive, zero, and NaN keys.
-- About 1 in 500 rows gets a NaN key to ensure NaN groups exist and
-- participate in the top-k heap eviction logic.
INSERT INTO t_gbylimit_float
SELECT
    if(number % 500 = 0, nan, ((number * 7 + 13) % 40000 - 20000)::Float32 / 7.0),
    if(number % 500 = 1, nan, ((number * 11 + 3) % 60000 - 30000)::Float64 / 11.0),
    number
FROM numbers(50000);

-- =====================
-- Float32 ASC
-- =====================
SELECT 'float32_asc';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Float32 DESC
-- =====================
SELECT 'float32_desc';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Float64 ASC
-- =====================
SELECT 'float64_asc';
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Float64 DESC
-- =====================
SELECT 'float64_desc';
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Float32 ASC NULLS FIRST  (NaN sorts first, like NULL)
-- =====================
SELECT 'float32_asc_nulls_first';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Float32 ASC NULLS LAST  (NaN sorts last)
-- =====================
SELECT 'float32_asc_nulls_last';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Float32 DESC NULLS FIRST  (NaN sorts first)
-- =====================
SELECT 'float32_desc_nulls_first';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Float32 DESC NULLS LAST  (NaN sorts last)
-- =====================
SELECT 'float32_desc_nulls_last';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Float64 ASC NULLS FIRST
-- =====================
SELECT 'float64_asc_nulls_first';
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- Float64 DESC NULLS LAST
-- =====================
SELECT 'float64_desc_nulls_last';
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_gbylimit_float;

-- ===================================================================
-- DateTime tests
-- ===================================================================

DROP TABLE IF EXISTS t_gbylimit_datetime;

CREATE TABLE t_gbylimit_datetime
(
    k_dt DateTime,
    val UInt64
) ENGINE = MergeTree ORDER BY val;

-- Generate rows with DateTime keys spanning a wide range (2010 .. ~2025).
INSERT INTO t_gbylimit_datetime
SELECT
    toDateTime('2010-01-01 00:00:00') + INTERVAL (number * 97 + 17) SECOND,
    number
FROM numbers(50000);

-- =====================
-- DateTime ASC
-- =====================
SELECT 'datetime_asc';
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

-- =====================
-- DateTime DESC
-- =====================
SELECT 'datetime_desc';
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_gbylimit_datetime;
