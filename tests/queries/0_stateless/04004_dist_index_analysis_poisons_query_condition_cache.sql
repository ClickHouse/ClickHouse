-- Tags: no-parallel, long
-- Tag no-parallel: Messes with internal cache

-- Test that distributed index analysis does not poison the query condition cache.
--
-- The bug: `filterPartsByQueryConditionCache` runs before distributed index analysis
-- and modifies `res_parts`. Then `distributedIndexAnalysisOnReplicas` returns results
-- via an `unordered_map`, so `result.parts_with_ranges` is built in arbitrary order.
-- The diff computation (which writes excluded ranges into the condition cache) iterates
-- `res_parts` and `result.parts_with_ranges` in parallel assuming both are sorted by
-- `part_index_in_query`. When `result.parts_with_ranges` is not sorted, parts get
-- mismatched and incorrectly treated as "erased entirely", writing all their marks as
-- non-matching into the condition cache. Subsequent queries then skip those parts.

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS t_dia_qcc;

-- Need enough parts for the condition cache to actually cache something,
-- and for distributed index analysis to activate.
-- Using small index_granularity so that each part has multiple granules.
CREATE TABLE t_dia_qcc (key Int64, value Int64)
ENGINE = MergeTree()
ORDER BY key
SETTINGS
    index_granularity = 256,
    min_bytes_for_wide_part = '1G',
    index_granularity_bytes = '10M',
    distributed_index_analysis_min_parts_to_activate = 0,
    distributed_index_analysis_min_indexes_bytes_to_activate = 0;

SYSTEM STOP MERGES t_dia_qcc;

-- Insert data in many blocks to get many parts.
-- 20 parts x 50000 rows = 1M rows. With index_granularity=256, each part has ~195 granules.
INSERT INTO t_dia_qcc SELECT number, number FROM numbers(1000000)
    SETTINGS max_block_size = 50000, min_insert_block_size_rows = 50000, max_insert_threads = 1;

SELECT count() AS parts FROM system.parts WHERE database = currentDatabase() AND table = 't_dia_qcc' AND active;

-- Step 1: Run query with QCC enabled but WITHOUT distributed index analysis to populate the cache.
SELECT count() FROM t_dia_qcc WHERE value > 300000 AND value < 700000
    SETTINGS use_query_condition_cache = 1, distributed_index_analysis = 0;

-- Confirm cache is populated.
SELECT count() > 0 AS cache_populated FROM system.query_condition_cache;

-- Step 2: Run the same query with distributed index analysis enabled.
-- This triggers the bug: the diff computation between `res_parts` (sorted) and
-- `result.parts_with_ranges` (unordered_map iteration order) mismatches parts,
-- writing their full ranges as non-matching into the condition cache.
SELECT count() FROM t_dia_qcc WHERE value > 300000 AND value < 700000
    SETTINGS
        use_query_condition_cache = 1,
        distributed_index_analysis = 1,
        distributed_index_analysis_for_non_shared_merge_tree = 1,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
        max_parallel_replicas = 2,
        allow_experimental_parallel_reading_from_replicas = 0;

-- Step 3: Run the query again WITHOUT distributed index analysis.
-- If the cache was poisoned, this will return fewer rows than expected.
SELECT count() FROM t_dia_qcc WHERE value > 300000 AND value < 700000
    SETTINGS use_query_condition_cache = 1, distributed_index_analysis = 0;

-- All three queries must return the same count: 399999
-- (values 300001..699999 inclusive)

DROP TABLE t_dia_qcc;
