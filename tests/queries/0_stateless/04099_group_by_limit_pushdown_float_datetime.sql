-- Correctness of enable_group_by_top_k_optimization for Float32/Float64 (NaN handling)
-- and DateTime key types on the typed numeric fast path in `TopKAggregationHeap`.

-- Tags: no-parallel-replicas

SET max_rows_to_group_by = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;

SET enable_group_by_top_k_optimization = 1;

DROP TABLE IF EXISTS t_gbylimit_float;

CREATE TABLE t_gbylimit_float
(
    k_f32 Float32,
    k_f64 Float64,
    val UInt64
) ENGINE = MergeTree ORDER BY val;

INSERT INTO t_gbylimit_float
SELECT
    if(number % 500 = 0, nan, ((number * 7 + 13) % 40000 - 20000)::Float32 / 7.0),
    if(number % 500 = 1, nan, ((number * 11 + 3) % 60000 - 30000)::Float64 / 11.0),
    number
FROM numbers(50000);

SELECT 'float32_asc';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float32_desc';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float64_asc';
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float64_desc';
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float32_asc_nulls_first';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float32_asc_nulls_last';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float32_desc_nulls_first';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float32_desc_nulls_last';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float64_asc_nulls_first';
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float64_desc_nulls_last';
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_gbylimit_float;

DROP TABLE IF EXISTS t_gbylimit_datetime;

CREATE TABLE t_gbylimit_datetime
(
    k_dt DateTime,
    val UInt64
) ENGINE = MergeTree ORDER BY val;

INSERT INTO t_gbylimit_datetime
SELECT
    toDateTime('2010-01-01 00:00:00') + INTERVAL (number * 97 + 17) SECOND,
    number
FROM numbers(50000);

SELECT 'datetime_asc';
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'datetime_desc';
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_gbylimit_datetime;

SELECT 'optimization_applied_guard';
SELECT count() FROM (EXPLAIN actions = 1 SELECT number AS k FROM numbers(100) GROUP BY k ORDER BY k LIMIT 5) WHERE explain LIKE '%Top-K%';
