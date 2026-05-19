-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache

-- Does additional QCC lookups that the test doesn't expect
SET automatic_parallel_replicas_mode = 0, enable_parallel_replicas = 0;
SET parallel_replicas_local_plan = 1;

SET allow_experimental_analyzer = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;

-- Prove that the query condition cache now records unmatched granules at individual-granule
-- granularity, even within batches that partially pass PREWHERE.
--
-- Setup: 100 granules of 1000 rows each. Only granule 1 (rows 1000-1999) has b = 1.
-- max_block_size = 1_000_000 forces all 100 granules into one read batch.
--
-- Old code: because the batch has row_count > 0 (granule 1 passes PREWHERE), none of the
-- 99 non-matching granules are recorded in the cache. On the second run all 100 marks are
-- re-read (SelectedMarks = 100).
--
-- New code: rows_per_granule[i] == 0 for granules 0 and 2-99, so those 99 marks are
-- written to the cache as unmatched. On the second run only mark 1 is selected
-- (SelectedMarks = 1).

SYSTEM CLEAR QUERY CONDITION CACHE;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 1000;

-- 100 granules; only granule 1 (rows 1000-1999) has b = 1.
INSERT INTO tab SELECT number, if(number >= 1000 AND number < 2000, 1, 0) FROM numbers(100_000);

-- First run: populates the cache.
-- All 100 granules fit in one batch (max_block_size >> total rows, preferred_block_size_bytes = 0).
-- The new per-granule logic records marks 0 and 2-99 as unmatched.
SELECT a+1, b+1, a*b FROM tab WHERE b = 1 FORMAT Null
SETTINGS use_query_condition_cache = true, max_block_size = 1000000, preferred_block_size_bytes = 0,
         log_comment = '04249_qcc_fine_grained_first_run';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryConditionCacheMisses'] AS cache_misses, ProfileEvents['SelectedMarks'] AS selected_marks
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = '04249_qcc_fine_grained_first_run'
ORDER BY event_time_microseconds;

-- Second run: uses the cache.
-- With fine-grained per-granule updates the cache marks 99 granules as non-matching,
-- so filterPartsByQueryConditionCache reduces the read to only mark 1 (SelectedMarks = 1).
SELECT a-1, b-1, a*a*b FROM tab WHERE b = 1 FORMAT Null
SETTINGS use_query_condition_cache = true, max_block_size = 1000000, preferred_block_size_bytes = 0,
         log_comment = '04249_qcc_fine_grained_second_run';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryConditionCacheHits'] AS cache_hits, ProfileEvents['SelectedMarks'] AS selected_marks
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = '04249_qcc_fine_grained_second_run'
ORDER BY event_time_microseconds;

DROP TABLE tab;
