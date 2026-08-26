-- Tags: no-fasttest, no-parallel
-- Tag no-fasttest: requires S3/minio-backed storage with a filesystem cache.
-- Tag no-parallel: the cold->warm assertion needs the cold read's populate to reserve cache space
-- and survive to the warm read. The dedicated `s3_cache_04511` policy isolates it from other tests'
-- background-merge cache traffic (which saturates the shared `s3_cache` with non-releasable segments
-- so the populate can't reserve); no-parallel additionally keeps the test's own flaky-check reruns
-- from contending that dedicated cache.

DROP TABLE IF EXISTS t_re_disk_cache;

CREATE TABLE t_re_disk_cache (k UInt64, v String)
ENGINE = MergeTree ORDER BY k
SETTINGS storage_policy = 's3_cache_04511', min_bytes_for_wide_part = 0;

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

-- One reading thread, so the cold read populates the cache completely and the warm read is a pure
-- cache hit. With several threads each stream gets its own `ReaderExecutor` over the same file, and
-- `DiskCacheWriter` appends only at the segment's live write offset: whichever stream loses the
-- downloader race contributes nothing, so the cold read leaves the segment PARTIALLY populated. The
-- warm read then re-fetches such a segment from its start (`claimLeadRole`'s `available` prefix is
-- deliberately unused - see the "Coarse by design" note in `ReaderExecutor::readThroughCaches`), which
-- can cost MORE source bytes than the cold read did and made the assertion below flaky.
SET max_threads = 1;

-- Cold read: nothing cached yet, so the executor reads from source and populates the cache. Warm read:
-- the same bytes must now be served from the cache. Both aggregates prove the served bytes are correct.
SELECT count(), sum(k) FROM t_re_disk_cache SETTINGS log_comment = 'reader_executor_cold';
SELECT count(), sum(k) FROM t_re_disk_cache SETTINGS log_comment = 'reader_executor_warm';

SYSTEM FLUSH LOGS query_log;

-- The read-through contract end to end on the real DiskCacheProvider: the cold read pulled bytes from
-- source (so the executor engaged) AND populated the cache ITSELF (`ReaderExecutorCachePopulateRequests`
-- > 0, not just the disk's incidental cache-on-read), and the warm reread then served every byte from
-- that cache, touching the source not at all. These counters are emitted only by the executor.
-- `warm = 0` rather than `warm < cold`: the latter compares two source-byte totals that both include
-- over-read, so it is a proxy that a partially populated cache can invert.
SELECT check_name, ok
FROM
(
    SELECT row.1 AS idx, row.2 AS check_name, row.3 AS ok
    FROM
    (
        SELECT arrayJoin([
            (1, 'cold_read_from_source', cold > 0),
            (2, 'executor_populated_cache', cold_pop > 0),
            (3, 'warm_served_from_cache', warm = 0)
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
