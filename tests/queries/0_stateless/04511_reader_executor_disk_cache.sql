-- Tags: no-fasttest
-- Tag no-fasttest: requires S3/minio-backed storage with a filesystem cache.

DROP TABLE IF EXISTS t_re_disk_cache;

CREATE TABLE t_re_disk_cache (k UInt64, v String)
ENGINE = MergeTree ORDER BY k
SETTINGS storage_policy = 's3_cache', min_bytes_for_wide_part = 0;

INSERT INTO t_re_disk_cache SELECT number, toString(number) FROM numbers(200000);

-- Engage the experimental executor with the filesystem cache. `read` avoids the async-prefetch
-- stage (which the executor does not implement yet and would force a fallback); the cache stays on.
SET use_reader_executor = 1;
SET remote_filesystem_read_method = 'read';
SET enable_filesystem_cache = 1;

-- Correct aggregates prove the bytes served through the cache chain are right.
SELECT count(), sum(k) FROM t_re_disk_cache;

SYSTEM FLUSH LOGS query_log;

-- `ReaderExecutorCacheGetRequests` is emitted only by the executor's cache chain, so a positive
-- sum over this test's queries proves the executor engaged AND consulted the filesystem cache
-- (rather than falling back to the legacy read path).
SELECT sum(ProfileEvents['ReaderExecutorCacheGetRequests']) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= today() - 1;

DROP TABLE t_re_disk_cache;
