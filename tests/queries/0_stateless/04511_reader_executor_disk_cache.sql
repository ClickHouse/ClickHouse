-- Tags: no-fasttest
-- Tag no-fasttest: requires S3/minio-backed storage with a filesystem cache.

DROP TABLE IF EXISTS t_re_disk_cache;

CREATE TABLE t_re_disk_cache (k UInt64, v String)
ENGINE = MergeTree ORDER BY k
SETTINGS storage_policy = 's3_cache', min_bytes_for_wide_part = 0;

-- No cache-on-write, so the first SELECT below is a genuine cold read that must populate the cache.
INSERT INTO t_re_disk_cache SELECT number, toString(number) FROM numbers(200000)
SETTINGS enable_filesystem_cache_on_write_operations = 0;

-- Engage the experimental executor with the filesystem cache. `read` avoids the async-prefetch
-- stage (which the executor does not implement yet and would force a fallback); the cache stays on.
SET use_reader_executor = 1;
SET remote_filesystem_read_method = 'read';
SET enable_filesystem_cache = 1;

-- Cold read: nothing cached yet, so the executor reads from source and populates the cache. Warm read:
-- the same bytes must now be served from the cache. Both aggregates prove the served bytes are correct.
SELECT count(), sum(k) FROM t_re_disk_cache SETTINGS log_comment = 'reader_executor_cold';
SELECT count(), sum(k) FROM t_re_disk_cache SETTINGS log_comment = 'reader_executor_warm';

SYSTEM FLUSH LOGS query_log;

-- The read-through contract end to end on the real DiskCacheProvider: the cold read pulled bytes from
-- source (so the executor engaged), and the warm reread served them from the cache, reading strictly
-- fewer source bytes. `ReaderExecutorBytesFromSource` is emitted only by the executor.
SELECT
    cold > 0 AS cold_read_from_source,
    warm < cold AS warm_served_from_cache
FROM
(
    SELECT
        sumIf(ProfileEvents['ReaderExecutorBytesFromSource'], log_comment = 'reader_executor_cold') AS cold,
        sumIf(ProfileEvents['ReaderExecutorBytesFromSource'], log_comment = 'reader_executor_warm') AS warm
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND type = 'QueryFinish'
      AND event_date >= today() - 1
);

DROP TABLE t_re_disk_cache;
