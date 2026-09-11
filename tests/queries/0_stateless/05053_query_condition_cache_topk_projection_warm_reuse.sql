-- Tags: long, no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache
-- Tag long: needs ~1M rows for the query condition cache to populate.
--
-- A projection-backed `ORDER BY ... LIMIT n` (TopK) read records the granules its `PREWHERE` rejected
-- in the query condition cache. Those entries are keyed by the `PREWHERE` condition, so the projection
-- candidate analysis in `optimizeUseNormalProjections` - the only place a warm query consults the cache
-- before the plan is final - must derive the projection `PREWHERE` before the analysis, otherwise it
-- probes the plain `WHERE` key only and the second run rescans everything.

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;
SET use_query_condition_cache_for_top_k = 1;
-- `force_optimize_projection` only checks (and the rewrite only runs) when projections are allowed at
-- all, so pin `optimize_use_projections`: the test settings randomization turns it off.
SET optimize_use_projections = 1;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET optimize_move_to_prewhere = 0;
-- Reading the projection in `v1` order lets the read stop before the dynamic `__topKFilter` has a
-- chance to reject whole granules, so nothing would be recorded. Pin the setting: its default (and the
-- test settings randomization) would otherwise decide whether entries appear at all.
SET optimize_read_in_order = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_threads = 1;
SET max_block_size = 8192;

DROP TABLE IF EXISTS tab_topk_proj;

CREATE TABLE tab_topk_proj
(
    id UInt32,
    v1 UInt32,
    v2 UInt32,
    PROJECTION proj (SELECT v1, v2 ORDER BY v1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab_topk_proj SELECT rand(), number, number FROM numbers(1_000_000);

-- Query condition cache entries are keyed by part; a background merge between the queries would drop
-- them and make the reuse flaky.
SYSTEM STOP MERGES tab_topk_proj;
SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT '--- A warm projection-backed TopK read reuses the granules the first run excluded';

SELECT v1 FROM tab_topk_proj WHERE v2 >= 0 ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS force_optimize_projection = 1, log_comment = '05053_topk_projection_prime';
SELECT v1 FROM tab_topk_proj WHERE v2 >= 0 ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS force_optimize_projection = 1, log_comment = '05053_topk_projection_warm';

SYSTEM FLUSH LOGS query_log;

-- Columns: (a query condition cache hit, the projection was used). Expected: 0,1 for the priming run
-- and 1,1 for the warm run.
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0,
    arrayExists(p -> p LIKE '%.proj', projections)
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('05053_topk_projection_prime', '05053_topk_projection_warm')
ORDER BY event_time_microseconds;

SELECT '--- The warm read still returns the correct rows';
SELECT v1 FROM tab_topk_proj WHERE v2 >= 0 ORDER BY v1 ASC LIMIT 5 SETTINGS force_optimize_projection = 1;

DROP TABLE tab_topk_proj;
