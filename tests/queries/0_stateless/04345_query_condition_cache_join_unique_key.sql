-- Tags: no-parallel-replicas, no-object-storage, no-s3-storage, no-shared-merge-tree, no-replicated-database, no-ordinary-database, no-fasttest
-- UNIQUE KEY requires a local-only storage policy (non-local disks raise SUPPORT_IS_DISABLED), an
-- Atomic database (UUID), and is not supported on SharedMergeTree, so this test is restricted to the
-- local-disk lanes the way the other UNIQUE KEY stateless tests are. It also inserts, and the UNIQUE
-- KEY index writer requires RocksDB (`SSTIndexWriter` throws SUPPORT_IS_DISABLED when USE_ROCKSDB is
-- off), which the Fast test build does not have, hence `no-fasttest` as in the other UNIQUE KEY tests
-- that insert.
--
-- Regression test for the UNIQUE KEY missing-rows hazard of the issue #104985 follow-up fix.
--
-- The #104985 fix re-runs the query condition cache mark trimming in updatePrewhereInfo so a
-- join-branch read consults the cache under the PREWHERE key. That re-trim runs during
-- optimizePrewhere, before initializePipeline finalizes reader_settings, so it must apply the same
-- eligibility gates the regular consult/skip path uses. The query condition cache is CSN-oblivious
-- and server-shared: trimming marks on a UNIQUE KEY read can make a reader pinned at an older
-- snapshot skip a mark whose rows are live at its CSN -> missing rows. The re-trim is therefore
-- gated on !hasUniqueKey() (mirroring the consult-side guard in selectRangesToReadImpl), so a
-- join-branch read of a UNIQUE KEY table must return the same rows with the cache on as with it off.

SET allow_experimental_unique_key = 1;
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET enable_parallel_replicas = 0;
SET async_insert = 0;

DROP TABLE IF EXISTS t_qcc_uk;

CREATE TABLE t_qcc_uk (id UInt64, x String)
ENGINE = MergeTree UNIQUE KEY (id) ORDER BY (id)
SETTINGS index_granularity = 8192;

-- 200k original rows, then upsert the first 100k to a new marker. The dedup writes delete-bitmaps
-- over the original parts, so a CSN-oblivious cache that drops marks could lose live rows.
INSERT INTO t_qcc_uk SELECT number, 'orig' FROM numbers(200000);
INSERT INTO t_qcc_uk SELECT number, 'updated_104985_uk' FROM numbers(100000);

-- Ground truth without the cache: the upserted marker matches exactly 100000 deduplicated rows.
SELECT count() FROM (
    SELECT a.id FROM t_qcc_uk AS a
    INNER JOIN t_qcc_uk AS b ON a.id = b.id
    WHERE a.x = 'updated_104985_uk' AND b.x = 'updated_104985_uk'
) SETTINGS use_query_condition_cache = 0;

-- Cold run with the cache on: must match the ground truth (the re-trim must be a no-op for UNIQUE KEY).
SELECT count() FROM (
    SELECT a.id FROM t_qcc_uk AS a
    INNER JOIN t_qcc_uk AS b ON a.id = b.id
    WHERE a.x = 'updated_104985_uk' AND b.x = 'updated_104985_uk'
) SETTINGS use_query_condition_cache = 1, log_comment = 'qcc_join_104985_uk';

-- Warm run with the cache on: still must match the ground truth, no rows lost to a stale cache entry.
SELECT count() FROM (
    SELECT a.id FROM t_qcc_uk AS a
    INNER JOIN t_qcc_uk AS b ON a.id = b.id
    WHERE a.x = 'updated_104985_uk' AND b.x = 'updated_104985_uk'
) SETTINGS use_query_condition_cache = 1, log_comment = 'qcc_join_104985_uk';

SYSTEM FLUSH LOGS query_log;

-- Matching row counts alone do not prove the guard is in place: the cache would have to both trim a
-- mark and be read at an older snapshot to lose rows, which this single-snapshot test cannot stage.
-- Assert the guard directly instead, namely that a UNIQUE KEY read consults the cache zero times, so
-- removing the eligibility gate is observable here. Expected, one row per cache-on run:
--   0  0   (no hit, no miss: the cache was never consulted)
SELECT
    ProfileEvents['QueryConditionCacheHits'],
    ProfileEvents['QueryConditionCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday()
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'qcc_join_104985_uk'
ORDER BY event_time_microseconds;

DROP TABLE t_qcc_uk;
