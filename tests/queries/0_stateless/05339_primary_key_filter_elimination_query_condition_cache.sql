-- Exercise cache reuse with local filter elimination even in the distributed-plan suite.
SET serialize_query_plan = 0,
    enable_parallel_replicas = 0,
    optimize_use_projections = 0,
    optimize_use_implicit_projections = 0,
    optimize_read_in_order = 0,
    optimize_aggregation_in_order = 0,
    optimize_move_to_prewhere = 0,
    compile_expressions = 0,
    use_query_cache = 0,
    use_query_condition_cache = 1,
    max_rows_to_read = 0,
    max_rows_to_read_leaf = 0,
    max_threads = 2,
    max_block_size = 7,
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    query_plan_optimize_primary_key_filter_elimination = 1;

DROP TABLE IF EXISTS pk_filter_cache;
CREATE TABLE pk_filter_cache (tenant UInt32, k UInt64, amount UInt64)
ENGINE = MergeTree ORDER BY (tenant, k)
SETTINGS index_granularity = 8, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES pk_filter_cache;
INSERT INTO pk_filter_cache SELECT 1, number * 2, number + 1 FROM numbers(256)
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

-- Odd keys are absent but fall inside a candidate granule. Each predicate has
-- its own cache entry; no global cache flush is needed for this fresh table.
SELECT sum(amount) FROM pk_filter_cache WHERE k = 1
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0 FORMAT Null;
SELECT 'where_disabled', sum(amount) FROM pk_filter_cache WHERE k = 1
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0, max_rows_to_read = 1;
SELECT 'where_enabled', sum(amount) FROM pk_filter_cache WHERE k = 1
SETTINGS max_rows_to_read = 1;

-- The cache identity of an explicit `PREWHERE` is distinct from a reconstructed
-- conjunction used to prove exact ranges. Both paths must retain the warm hit.
SELECT sum(amount) FROM pk_filter_cache PREWHERE k = 3
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0 FORMAT Null;
SELECT 'prewhere_disabled', sum(amount) FROM pk_filter_cache PREWHERE k = 3
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0, max_rows_to_read = 1;
SELECT 'prewhere_enabled', sum(amount) FROM pk_filter_cache PREWHERE k = 3
SETTINGS max_rows_to_read = 1;

SELECT sum(amount) FROM pk_filter_cache PREWHERE tenant = 1 WHERE k = 5
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0 FORMAT Null;
SELECT 'combined_disabled', sum(amount) FROM pk_filter_cache PREWHERE tenant = 1 WHERE k = 5
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0, max_rows_to_read = 1;
SELECT 'combined_enabled', sum(amount) FROM pk_filter_cache PREWHERE tenant = 1 WHERE k = 5
SETTINGS max_rows_to_read = 1;

SELECT sum(amount) FROM pk_filter_cache WHERE k = 9
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0, optimize_move_to_prewhere = 1 FORMAT Null;
SELECT 'automatic_prewhere_disabled', sum(amount) FROM pk_filter_cache WHERE k = 9
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0, optimize_move_to_prewhere = 1, max_rows_to_read = 1;
SELECT 'automatic_prewhere_enabled', sum(amount) FROM pk_filter_cache WHERE k = 9
SETTINGS optimize_move_to_prewhere = 1, max_rows_to_read = 1;

-- An unseen condition still requires reading a granule, so a real read limit
-- must not disappear while speculative exact-range analysis defers its checks.
SELECT sum(amount) FROM pk_filter_cache WHERE k = 7
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0, max_rows_to_read = 1; -- { serverError TOO_MANY_ROWS }
SELECT sum(amount) FROM pk_filter_cache WHERE k = 7
SETTINGS max_rows_to_read = 1; -- { serverError TOO_MANY_ROWS }

-- Cache use must also coexist with a profitable rewrite, including the
-- original `PREWHERE` identity and its narrower `WHERE` filter.
SELECT 'range_disabled', sum(amount) FROM pk_filter_cache
WHERE tenant = 1 AND k >= 13 AND k < 243
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0;
SELECT 'range_plan', countIf(explain LIKE '%Primary key proven filter%') > 0
FROM (EXPLAIN description = 1 SELECT sum(amount) FROM pk_filter_cache
    WHERE tenant = 1 AND k >= 13 AND k < 243);
SELECT 'range_enabled', sum(amount) FROM pk_filter_cache
WHERE tenant = 1 AND k >= 13 AND k < 243;

SELECT 'prewhere_range_disabled', sum(amount) FROM pk_filter_cache
PREWHERE tenant = 1 AND k >= 13 AND k < 243
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0;
SELECT 'prewhere_range_plan', countIf(explain LIKE '%Primary key proven filter%') > 0
FROM (EXPLAIN description = 1 SELECT sum(amount) FROM pk_filter_cache
    PREWHERE tenant = 1 AND k >= 13 AND k < 243);
SELECT 'prewhere_range_enabled', sum(amount) FROM pk_filter_cache
PREWHERE tenant = 1 AND k >= 13 AND k < 243;

SELECT 'combined_range_disabled', sum(amount) FROM pk_filter_cache
PREWHERE tenant = 1 WHERE k >= 13 AND k < 243
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0;
SELECT 'combined_range_plan', countIf(explain LIKE '%Primary key proven filter%') > 0
FROM (EXPLAIN description = 1 SELECT sum(amount) FROM pk_filter_cache
    PREWHERE tenant = 1 WHERE k >= 13 AND k < 243);
SELECT 'combined_range_enabled', sum(amount) FROM pk_filter_cache
PREWHERE tenant = 1 WHERE k >= 13 AND k < 243;

DROP TABLE pk_filter_cache;

-- A condition on the second key selects at least sixteen candidate granules.
-- Half contain one matching row; the cache removes the intervening empty ones.
-- Enough granules remain for proof analysis, but none is wholly matching. Its
-- speculative analysis must not reject the ordinary cached read's limit
-- before declining the unprofitable rewrite.
SET merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0;
DROP TABLE IF EXISTS pk_filter_cache_gaps;
CREATE TABLE pk_filter_cache_gaps (a UInt32, b UInt32, amount UInt64)
ENGINE = MergeTree ORDER BY (a, b)
SETTINGS index_granularity = 8, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES pk_filter_cache_gaps;
INSERT INTO pk_filter_cache_gaps
SELECT intDiv(number, 8), if(intDiv(number, 8) % 2 = 0, number % 8, (number % 8) * 2), number + 1
FROM numbers(256)
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;
SELECT sum(amount) FROM pk_filter_cache_gaps WHERE b = 1
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0 FORMAT Null;
SELECT 'cache_gaps_disabled', sum(amount) FROM pk_filter_cache_gaps WHERE b = 1
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0, max_rows_to_read = 128;
SELECT 'cache_gaps_enabled', sum(amount) FROM pk_filter_cache_gaps WHERE b = 1
SETTINGS max_rows_to_read = 128;
DROP TABLE pk_filter_cache_gaps;
