-- Tags: no-parallel, no-random-settings, no-s3-storage, no-azure-blob-storage

-- Regression for the lazy-materialization second phase: the automatic uncompressed-cache
-- decision must be made against the rows actually read in that phase, not against the
-- pre-limit first-phase scan. A large first pass (ORDER BY a non-key column) that exceeds
-- `merge_tree_max_rows_to_use_cache` must not disable caching for a small repeated payload
-- read. Before the fix the decision was frozen off by the first-phase scan, so the warm
-- run produced no `UncompressedCacheHits`.

SET enable_automatic_use_uncompressed_cache = 1;
SET enable_analyzer = 1;
SET log_queries = 1;
SET optimize_read_in_order = 1;
SET parallel_replicas_local_plan = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 10000;

-- Force the first-phase scan to exceed the cache thresholds while the second phase reads
-- only a handful of rows.
SET merge_tree_max_rows_to_use_cache = 25600;

DROP TABLE IF EXISTS lazy_mat_uc_cache_large_scan;

CREATE TABLE lazy_mat_uc_cache_large_scan
(
    id UInt64,
    score UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 256, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- 100000 rows / 256 = 391 marks for the first-phase scan, well above the 100-mark cache
-- threshold (25600 / 256). `score` is uncorrelated with the primary key, so ORDER BY forces
-- a full first-phase scan; only 10 rows survive the LIMIT for the second-phase payload read.
INSERT INTO lazy_mat_uc_cache_large_scan
SELECT number, cityHash64(number), repeat('x', 1024)
FROM numbers(100000);

SYSTEM DROP UNCOMPRESSED CACHE;

-- The plan must use lazy materialization for `payload`.
SELECT count() > 0
FROM
(
    SELECT trimLeft(explain) AS explain
    FROM
    (
        EXPLAIN PLAN actions=1
        SELECT id, payload
        FROM lazy_mat_uc_cache_large_scan
        ORDER BY score
        LIMIT 10
        SETTINGS max_threads = 1
    )
)
WHERE explain LIKE '%LazilyRead%'
   OR explain LIKE '%Lazily read columns:%';

SELECT id, payload
FROM lazy_mat_uc_cache_large_scan
ORDER BY score
LIMIT 10
FORMAT Null
SETTINGS max_threads = 1, log_comment = '04499_lazy_materialization_uncompressed_cache_large_scan_run_1';

SYSTEM FLUSH LOGS query_log;

-- Cold cache: the second-phase payload read populates the uncompressed cache.
SELECT ProfileEvents['UncompressedCacheMisses'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '04499_lazy_materialization_uncompressed_cache_large_scan_run_1'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT id, payload
FROM lazy_mat_uc_cache_large_scan
ORDER BY score
LIMIT 10
FORMAT Null
SETTINGS max_threads = 1, log_comment = '04499_lazy_materialization_uncompressed_cache_large_scan_run_2';

SYSTEM FLUSH LOGS query_log;

-- Warm cache: the repeated small payload read now hits the cache.
SELECT ProfileEvents['UncompressedCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '04499_lazy_materialization_uncompressed_cache_large_scan_run_2'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE lazy_mat_uc_cache_large_scan;
