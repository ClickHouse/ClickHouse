-- `__topKFilter` drops the rows beyond the top-K boundary of the aggregation. NaN is placed first or last
-- by `NULLS FIRST/LAST`, so with `NULLS FIRST` it ranks before any threshold and must pass the filter:
-- the NaN group must keep all of its rows. Several blocks let the boundary be published before all NaN rows are read.

SET max_threads = 1;
SET max_block_size = 1000;
SET max_rows_to_group_by = 0;
SET serialize_query_plan = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_group_by_top_k_optimization = 1;
SET enable_group_by_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_top_k_nan;

CREATE TABLE t_top_k_nan
(
    f32 Float32,
    f64 Float64,
    val UInt64,
    INDEX f32_minmax f32 TYPE minmax GRANULARITY 1,
    INDEX f64_minmax f64 TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY val SETTINGS index_granularity = 1000;

INSERT INTO t_top_k_nan
SELECT if(number % 500 = 0, nan, number), if(number % 500 = 1, -nan, number), number
FROM numbers(50000);

SELECT 'f32 ASC NULLS FIRST';
SELECT f32, count() FROM t_top_k_nan GROUP BY f32 ORDER BY f32 ASC NULLS FIRST LIMIT 2;
SELECT 'f64 ASC NULLS FIRST';
SELECT f64, count() FROM t_top_k_nan GROUP BY f64 ORDER BY f64 ASC NULLS FIRST LIMIT 2;
SELECT 'f64 DESC NULLS FIRST';
SELECT f64, count() FROM t_top_k_nan GROUP BY f64 ORDER BY f64 DESC NULLS FIRST LIMIT 2;
SELECT 'f64 DESC';
SELECT f64, count() FROM t_top_k_nan GROUP BY f64 ORDER BY f64 DESC LIMIT 2;
SELECT 'f64 ASC';
SELECT f64, count() FROM t_top_k_nan GROUP BY f64 ORDER BY f64 ASC LIMIT 2;
SELECT 'f64 LIMIT without ORDER BY';
SELECT count() FROM (SELECT f64 FROM t_top_k_nan GROUP BY f64 LIMIT 2);

DROP TABLE t_top_k_nan;
