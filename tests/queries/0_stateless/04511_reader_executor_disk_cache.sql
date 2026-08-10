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
-- stage (which the executor does not implement, and which would force a fallback); the cache stays on.
-- Pin populate-on-miss: with `read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 1` the
-- executor's DiskCacheProvider is read-only and never populates, so the warm read would depend on the
-- disk's incidental cache-on-read instead of the path this test verifies. Pinning it to 0 makes the
-- randomized-settings runs (the flaky check) deterministic.
SET use_reader_executor = 1;
SET remote_filesystem_read_method = 'read';
SET enable_filesystem_cache = 1;
SET read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0;

-- Cold read: nothing cached yet, so the executor reads from source and populates the cache. Warm read:
-- the same bytes must now be served from the cache. Both aggregates prove the served bytes are correct.
SELECT count(), sum(k) FROM t_re_disk_cache SETTINGS log_comment = 'reader_executor_cold';
SELECT count(), sum(k) FROM t_re_disk_cache SETTINGS log_comment = 'reader_executor_warm';

SYSTEM FLUSH LOGS query_log;

-- The read-through contract end to end on the real DiskCacheProvider: the cold read pulled bytes from
-- source (so the executor engaged) AND populated the cache ITSELF (`ReaderExecutorCachePopulateRequests`
-- > 0, not just the disk's incidental cache-on-read), and the warm reread then served those bytes from
-- that cache, reading strictly fewer source bytes. These counters are emitted only by the executor.
SELECT check_name, ok
FROM
(
    SELECT row.1 AS idx, row.2 AS check_name, row.3 AS ok
    FROM
    (
        SELECT arrayJoin([
            (1, 'cold_read_from_source', cold > 0),
            (2, 'executor_populated_cache', cold_pop > 0),
            (3, 'warm_served_from_cache', warm < cold)
        ]) AS row
        FROM
        (
            SELECT
                sumIf(ProfileEvents['ReaderExecutorBytesFromSource'], log_comment = 'reader_executor_cold') AS cold,
                sumIf(ProfileEvents['ReaderExecutorBytesFromSource'], log_comment = 'reader_executor_warm') AS warm,
                sumIf(ProfileEvents['ReaderExecutorCachePopulateRequests'], log_comment = 'reader_executor_cold') AS cold_pop
            FROM system.query_log
            WHERE current_database = currentDatabase()
              AND type = 'QueryFinish'
              AND event_date >= today() - 1
        )
    )
)
ORDER BY idx;

DROP TABLE t_re_disk_cache;
