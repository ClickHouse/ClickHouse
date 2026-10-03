-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-object-storage, no-replicated-database, no-shared-merge-tree
-- Tag no-fasttest: the `local_remote` storage policy uses an S3-backed volume (requires MinIO).
-- Tag no-object-storage: with object storage everywhere the `local` volume is not local either.
-- Tag no-replicated-database: `ALTER TABLE ... MOVE PARTITION TO VOLUME` is not replicated.
-- Tag no-shared-merge-tree: all parts are on object storage there.

-- Regression for the lazy-materialization second phase of automatic uncompressed-cache mode:
-- `LazyMaterializingRows::filterRangesAndFillRows` drops parts whose ranges become empty, so a query
-- whose first phase scanned both local and remote parts can arrive at the second phase with only the
-- local parts left. Automatic mode must stay off for such a mixed read - the decision has to remember
-- that the first phase touched remote parts.

SET enable_automatic_use_uncompressed_cache = 1;
SET enable_analyzer = 1;
SET log_queries = 1;
SET optimize_read_in_order = 1;
SET parallel_replicas_local_plan = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 10000;

DROP TABLE IF EXISTS uc_cache_mixed_storage;

CREATE TABLE uc_cache_mixed_storage
(
    part_id UInt8,
    id UInt64,
    score UInt64,
    payload String
)
ENGINE = MergeTree
PARTITION BY part_id
ORDER BY id
SETTINGS storage_policy = 'local_remote',
         index_granularity = 256,
         index_granularity_bytes = 0,
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;

-- The low scores live in the partition that stays local, so the rows surviving `ORDER BY score LIMIT`
-- all come from the local partition while the first phase still scans the remote one.
INSERT INTO uc_cache_mixed_storage SELECT 0, number, number, repeat('x', 1024) FROM numbers(2048);
INSERT INTO uc_cache_mixed_storage SELECT 1, 1000000 + number, 1000000 + number, repeat('y', 1024) FROM numbers(2048);

ALTER TABLE uc_cache_mixed_storage MOVE PARTITION 1 TO VOLUME 'remote';

SELECT partition, disk_name != 'default' FROM system.parts
WHERE database = currentDatabase() AND table = 'uc_cache_mixed_storage' AND active
ORDER BY partition;

-- The plan must use lazy materialization for `payload`.
SELECT count() > 0
FROM
(
    SELECT trimLeft(explain) AS explain
    FROM
    (
        EXPLAIN PLAN actions = 1
        SELECT id, payload
        FROM uc_cache_mixed_storage
        ORDER BY score
        LIMIT 10
        SETTINGS max_threads = 1
    )
)
WHERE explain LIKE '%LazilyRead%'
   OR explain LIKE '%Lazily read columns:%';

-- The cache is deliberately not dropped here: the assertions below are about whether the
-- uncompressed cache is touched at all, not about hits versus misses, so the test needs no cold
-- cache - and dropping the server-wide cache would disturb tests running in parallel.
SELECT id, payload
FROM uc_cache_mixed_storage
ORDER BY score
LIMIT 10
FORMAT Null
SETTINGS max_threads = 1, log_comment = '04649_uncompressed_cache_auto_enable_mixed_run';

SELECT id, payload
FROM uc_cache_mixed_storage
ORDER BY score
LIMIT 10
FORMAT Null
SETTINGS max_threads = 1, log_comment = '04649_uncompressed_cache_auto_enable_mixed_run';

SYSTEM FLUSH LOGS query_log;

-- A mixed local + remote read never touches the uncompressed cache, in either phase.
SELECT sum(ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses'])
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '04649_uncompressed_cache_auto_enable_mixed_run';

-- Control: once every part is local again, the same query does use the cache automatically -
-- otherwise the assertion above would hold for the wrong reason.
ALTER TABLE uc_cache_mixed_storage MOVE PARTITION 1 TO VOLUME 'local';

SELECT id, payload
FROM uc_cache_mixed_storage
ORDER BY score
LIMIT 10
FORMAT Null
SETTINGS max_threads = 1, log_comment = '04649_uncompressed_cache_auto_enable_local_run';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '04649_uncompressed_cache_auto_enable_local_run'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE uc_cache_mixed_storage;
