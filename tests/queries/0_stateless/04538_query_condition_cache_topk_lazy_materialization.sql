-- Tags: long, no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache
-- Tag long: needs ~1M rows for the QCC to populate (a granule-spanning chunk must be
--   fully filtered before the LIMIT cancels the pipeline).
--
-- Regression for lazy materialization + TopK + QCC: selecting an extra column that is
-- not needed for ORDER BY or WHERE triggers `optimizeLazyMaterialization2`, which
-- replaces the `FilterStep` that received the QCC key. Without a second
-- `updateQueryConditionCache` pass after lazy materialization, the cache never
-- populates, so a re-run of the same plan cannot reuse anything.
--
-- The reuse is asserted from the read side via the `QueryConditionCacheHits` profile
-- event (and the dropped-granule effect via `SelectedMarks` < `SelectedMarksTotal`),
-- rather than from `count()` on `system.query_condition_cache`: if the first run never
-- populates the cache, the second run just re-misses and overwrites the same key, so the
-- entry count still reads `1` and the count-based check stays green even when the fix is
-- reverted.

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000;
SET optimize_move_to_prewhere = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;
SET max_threads = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id UInt32, v1 UInt32, v2 UInt32, extra UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab SELECT rand(), number, number, number * 2 FROM numbers(1_000_000);

-- Prove the plan uses both TopK dynamic filtering and lazy materialization.
SELECT
    (countIf(explain LIKE '%__topKFilter(v1)%') > 0)
AND (countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0)
FROM (
    EXPLAIN actions = 1
    SELECT v1, extra FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5);

SELECT '--- A lazy-materialized TopK plan populates the QCC and a re-run reuses it';
SYSTEM CLEAR QUERY CONDITION CACHE;

-- Cold run: cache miss, all granules read; the post-lazy-materialization pass writes the entry.
SELECT v1, extra FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS log_comment = '04538_cold';
-- Warm re-run of the same plan: cache hit, granules dropped.
SELECT v1, extra FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS log_comment = '04538_reuse';
-- A different LIMIT is a different TopK plan (salted key), so it must not reuse the entry: cache miss.
SELECT v1, extra FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 7 FORMAT Null SETTINGS log_comment = '04538_difflimit';

SYSTEM FLUSH LOGS query_log;

-- Columns: (any QCC hit), (granules skipped). Expected: cold = 0 0, reuse = 1 1, difflimit = 0 0.
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0,
    toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('04538_cold', '04538_reuse', '04538_difflimit')
ORDER BY event_time_microseconds;

DROP TABLE tab;
