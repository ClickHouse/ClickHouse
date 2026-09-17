-- Tags: long, no-parallel
-- Tag no-parallel: Messes with internal cache
-- Tag long: needs ~1M rows for the QCC to populate.
--
-- `optimizeUseNormalProjections` may replace a read that `tryOptimizeTopK` has already stamped
-- for TopK filtering with a fresh projection read. The replacement must observe the same query
-- condition cache (QCC) gating as the read it replaces: with
-- `use_query_condition_cache_for_top_k = 0`, the projection read must not write any QCC entry,
-- and the projection candidate analysis must not consult entries under the plain condition hash.
--
-- The write side is the observable discriminator: without carrying the gate onto the projection
-- read, its `PREWHERE` reader writes a plain-keyed entry for the TopK query. The consult side has
-- no observable discriminator today (entries for projection parts are written under a
-- `<parent_part>:<projection>` key while the analysis-time consult probes the bare projection
-- part name, so a probe cannot hit), and is gated for the invariant only.
--
-- The TopK stamp uses the skip-index-only shape (dynamic filtering off, minmax index on the sort
-- column): a `__topKFilter` node in the prewhere would be non-deterministic and already suppress
-- the reader-level write, hiding the gate.

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;
-- Turn the gate off: this test covers the contract of the query condition cache being
-- switched off for TopK reads.
SET use_query_condition_cache_for_top_k = 0;
-- `force_optimize_projection` only checks (and the rewrite only runs) when projections are allowed at
-- all, so pin `optimize_use_projections`: the test settings randomization turns it off.
SET optimize_use_projections = 1;
SET use_top_k_dynamic_filtering = 0;
SET use_skip_indexes_for_top_k = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET optimize_move_to_prewhere = 0;
SET parallel_replicas_local_plan = 1;
SET max_threads = 1;

DROP TABLE IF EXISTS tab_proj;

-- The projection is sorted by the TopK sort column `v1`, so it is usable for the
-- `ORDER BY v1 LIMIT n` query (`force_optimize_projection` below makes the choice deterministic);
-- its own primary key does not prune the `v2` predicate, so a projection read scans (and would
-- cache) every projection granule.
CREATE TABLE tab_proj
(
    id UInt32,
    v1 UInt32,
    v2 UInt32,
    INDEX idx_v1 v1 TYPE minmax GRANULARITY 1,
    PROJECTION proj (SELECT v1, v2 ORDER BY v1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab_proj SELECT rand(), number, number FROM numbers(1_000_000);

-- QCC entries are keyed by part; a background merge between the queries would drop entries and
-- make the entry counts flaky.
SYSTEM STOP MERGES tab_proj;

SELECT '--- A projection-backed TopK read must not write any QCC entry when the gate is off';
SYSTEM CLEAR QUERY CONDITION CACHE;

-- `tryOptimizeTopK` stamps the read via the minmax index on `v1`, then the projection rewrite
-- replaces it. The projection read carries the QCC gate of the read it replaced, so its
-- `PREWHERE` reader must not write an entry, and no run may hit.
SELECT v1 FROM tab_proj WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS force_optimize_projection = 1, log_comment = '04658_topk_write_1';
SELECT v1 FROM tab_proj WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS force_optimize_projection = 1, log_comment = '04658_topk_write_2';

-- Expected: 0 entries.
SELECT count() FROM system.query_condition_cache;

SYSTEM FLUSH LOGS query_log;

-- Columns: (any QCC hit, the projection was used). Expected: 0,1 for both runs.
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0,
    arrayExists(p -> p LIKE '%.proj', projections)
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('04658_topk_write_1', '04658_topk_write_2')
ORDER BY event_time_microseconds;

SELECT '--- Projection-backed TopK still returns the planted row';
SELECT v1 FROM tab_proj WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 SETTINGS force_optimize_projection = 1;

SELECT '--- Positive control: a plain projection read still writes QCC entries';
SYSTEM CLEAR QUERY CONDITION CACHE;

-- The same read without TopK is not gated: its `PREWHERE` reader writes entries, proving the
-- zero count above comes from the gate and not from a dead write path.
SELECT v1 FROM tab_proj WHERE v2 = 10000 FORMAT Null SETTINGS force_optimize_projection = 1, log_comment = '04658_plain_prime';

-- Expected: entries exist.
SELECT count() > 0 FROM system.query_condition_cache;

SYSTEM FLUSH LOGS query_log;

-- Columns: (the projection was used). Expected: 1.
SELECT
    log_comment,
    arrayExists(p -> p LIKE '%.proj', projections)
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = '04658_plain_prime'
ORDER BY event_time_microseconds;

DROP TABLE tab_proj;
