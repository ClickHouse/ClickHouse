-- Tags: no-fasttest
-- Tag no-fasttest: Depends on S3

-- Regression test for `query_cache_use_only_when_data_was_not_changed` over an object-storage (S3)
-- table. `StorageObjectStorage::getModificationHash` lists the objects behind the table independently
-- of the read, so the finalization consistency check could re-list the same object set even though the
-- read consumed a different one. To close that, the read records the object set it actually consumed
-- (`QueryConsumedObjectSets`) and `getModificationHash` hashes that set at finalization. The cache is
-- then populated only while the consumed set matches the object set folded into its key (so the second
-- run below is a cache hit), and invalidated when an object changes. See PR #108721 (review 3508863459).

-- Fixed object names, always overwritten, so the read set is exactly {1, 2} regardless of leftovers.
INSERT INTO FUNCTION s3(s3_conn, filename = 'test_04495_1', format = 'TSV', structure = 'x UInt64') SELECT 10 SETTINGS s3_truncate_on_insert = 1;
INSERT INTO FUNCTION s3(s3_conn, filename = 'test_04495_2', format = 'TSV', structure = 'x UInt64') SELECT 20 SETTINGS s3_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_s3_qc;
-- An S3-engine table has a UUID (unlike the `s3` table function), so it can report a modification hash.
CREATE TABLE t_s3_qc (x UInt64) ENGINE = S3(s3_conn, filename = 'test_04495_*', format = 'TSV');

-- First run stores the result (the read-consumed object set matches the pre-read listing).
SELECT sum(x) FROM t_s3_qc SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
-- Second run over the unchanged object set is served from the cache.
SELECT sum(x) FROM t_s3_qc SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;

-- Rewrite one object (new content, new ETag): the cache is invalidated and the result recomputed.
INSERT INTO FUNCTION s3(s3_conn, filename = 'test_04495_2', format = 'TSV', structure = 'x UInt64') SELECT number + 21 FROM numbers(3) SETTINGS s3_truncate_on_insert = 1;
SELECT sum(x) FROM t_s3_qc SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;

-- The second run must be a cache hit (1); the first run and the post-rewrite run must not (0). This is
-- the regression signal: the hit only happens when the consumed-set hash matches the pre-read key.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE '%SELECT sum(x) FROM t_s3_qc%'
ORDER BY event_time_microseconds;

DROP TABLE t_s3_qc;
