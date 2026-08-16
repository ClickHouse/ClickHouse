-- Tags: no-fasttest, no-parallel
-- Tag no-fasttest: requires S3/minio-backed storage with a filesystem cache.
-- Tag no-parallel: the read below must run while `file_cache_simulate_evicting_segment` is armed,
-- and that failpoint is server-global, so a concurrent test clearing it breaks the assertion.

DROP TABLE IF EXISTS t_re_detached;

CREATE TABLE t_re_detached (k UInt64, v String)
ENGINE = MergeTree ORDER BY k
SETTINGS storage_policy = 's3_cache_04907', min_bytes_for_wide_part = 0;

INSERT INTO t_re_detached SELECT number, toString(number) FROM numbers(100000)
SETTINGS enable_filesystem_cache_on_write_operations = 0;

SET use_reader_executor = 1;
SET remote_filesystem_read_method = 'read';
SET enable_filesystem_cache = 1;
-- Pin populate-on-miss: with this setting at 1 the executor's cache provider is read-only and never
-- reaches the code path under test.
SET read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0;

-- The failpoint makes every segment the cache finds report itself as evicting or removed, so
-- `getOrSet` hands the executor DETACHED placeholders. Such a segment holds no bytes and can never
-- accept any, so it must be read from source rather than assigned a cache writer.
SYSTEM ENABLE FAILPOINT file_cache_simulate_evicting_segment;
SELECT count(), sum(k) FROM t_re_detached;
SYSTEM DISABLE FAILPOINT file_cache_simulate_evicting_segment;

-- The same read with the failpoint cleared, to keep the ordinary populate path covered.
SELECT count(), sum(k) FROM t_re_detached;

DROP TABLE t_re_detached;
