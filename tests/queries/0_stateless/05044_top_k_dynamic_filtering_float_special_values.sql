-- Top-k dynamic filtering and skip-index top-k put floating point sort keys on the
-- lock-free `TopKThresholdTrackerNumeric` path, which has boundary logic of its own:
-- a `NaN` boundary must never become the threshold, and the initial threshold is
-- `+-inf` rather than the maximum finite value. Compare the optimized and the
-- unoptimized result for `Float64` / `Float32` data containing `nan`, `inf` and
-- `-inf`, for both the default `NULLS LAST` ordering and `NULLS FIRST` (which flips
-- where `NaN` sorts).

SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET use_query_condition_cache = 0;
SET use_top_k_dynamic_filtering_for_variable_length_types = 0;

DROP TABLE IF EXISTS tab_float;
CREATE TABLE tab_float
(
    id UInt32,
    f64 Float64,
    f32 Float32,
    INDEX f64_idx f64 TYPE minmax,
    INDEX f32_idx f32 TYPE minmax
) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;

-- Four rows each of -inf, inf and nan, scattered over different granules.
INSERT INTO tab_float
SELECT
    number,
    multiIf(number % 2500 = 7, -inf, number % 2500 = 13, inf, number % 2500 = 23, nan, toFloat64(number) - 5000.5) AS v,
    toFloat32(v)
FROM numbers(10000);

SELECT 'Float64 ASC unoptimized';
SELECT f64 FROM tab_float ORDER BY f64 ASC LIMIT 6 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'Float64 ASC optimized';
SELECT f64 FROM tab_float ORDER BY f64 ASC LIMIT 6 SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;

SELECT 'Float64 ASC NULLS FIRST unoptimized';
SELECT f64 FROM tab_float ORDER BY f64 ASC NULLS FIRST LIMIT 6 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'Float64 ASC NULLS FIRST optimized';
SELECT f64 FROM tab_float ORDER BY f64 ASC NULLS FIRST LIMIT 6 SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;

SELECT 'Float64 DESC unoptimized';
SELECT f64 FROM tab_float ORDER BY f64 DESC LIMIT 6 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'Float64 DESC optimized';
SELECT f64 FROM tab_float ORDER BY f64 DESC LIMIT 6 SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;

SELECT 'Float64 DESC NULLS FIRST unoptimized';
SELECT f64 FROM tab_float ORDER BY f64 DESC NULLS FIRST LIMIT 6 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'Float64 DESC NULLS FIRST optimized';
SELECT f64 FROM tab_float ORDER BY f64 DESC NULLS FIRST LIMIT 6 SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;

SELECT 'Float32 ASC unoptimized';
SELECT f32 FROM tab_float ORDER BY f32 ASC LIMIT 6 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'Float32 ASC optimized';
SELECT f32 FROM tab_float ORDER BY f32 ASC LIMIT 6 SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;

SELECT 'Float32 ASC NULLS FIRST unoptimized';
SELECT f32 FROM tab_float ORDER BY f32 ASC NULLS FIRST LIMIT 6 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'Float32 ASC NULLS FIRST optimized';
SELECT f32 FROM tab_float ORDER BY f32 ASC NULLS FIRST LIMIT 6 SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;

SELECT 'Float32 DESC unoptimized';
SELECT f32 FROM tab_float ORDER BY f32 DESC LIMIT 6 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'Float32 DESC optimized';
SELECT f32 FROM tab_float ORDER BY f32 DESC LIMIT 6 SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;

SELECT 'Float32 DESC NULLS FIRST unoptimized';
SELECT f32 FROM tab_float ORDER BY f32 DESC NULLS FIRST LIMIT 6 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'Float32 DESC NULLS FIRST optimized';
SELECT f32 FROM tab_float ORDER BY f32 DESC NULLS FIRST LIMIT 6 SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;

DROP TABLE tab_float;

-- The whole top-k consists of infinities, so the published threshold equals the
-- initial sentinel: it must not exclude the very values it was reached by.
DROP TABLE IF EXISTS tab_sentinel;
CREATE TABLE tab_sentinel
(
    id UInt32,
    pos_inf Float64,
    neg_inf Float64,
    INDEX pos_inf_idx pos_inf TYPE minmax,
    INDEX neg_inf_idx neg_inf TYPE minmax
) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;

INSERT INTO tab_sentinel SELECT number, if(number % 4 = 0, nan, inf), if(number % 4 = 0, nan, -inf) FROM numbers(1000);

SELECT 'inf/nan ASC unoptimized';
SELECT pos_inf FROM tab_sentinel ORDER BY pos_inf ASC LIMIT 4 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'inf/nan ASC optimized';
SELECT pos_inf FROM tab_sentinel ORDER BY pos_inf ASC LIMIT 4 SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;

SELECT 'inf/nan ASC NULLS FIRST unoptimized';
SELECT pos_inf FROM tab_sentinel ORDER BY pos_inf ASC NULLS FIRST LIMIT 4 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT 'inf/nan ASC NULLS FIRST optimized';
SELECT pos_inf FROM tab_sentinel ORDER BY pos_inf ASC NULLS FIRST LIMIT 4 SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;

SELECT '-inf/nan DESC unoptimized';
SELECT neg_inf FROM tab_sentinel ORDER BY neg_inf DESC LIMIT 4 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT '-inf/nan DESC optimized';
SELECT neg_inf FROM tab_sentinel ORDER BY neg_inf DESC LIMIT 4 SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;

SELECT '-inf/nan DESC NULLS FIRST unoptimized';
SELECT neg_inf FROM tab_sentinel ORDER BY neg_inf DESC NULLS FIRST LIMIT 4 SETTINGS use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0;
SELECT '-inf/nan DESC NULLS FIRST optimized';
SELECT neg_inf FROM tab_sentinel ORDER BY neg_inf DESC NULLS FIRST LIMIT 4 SETTINGS use_top_k_dynamic_filtering = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;

DROP TABLE tab_sentinel;
