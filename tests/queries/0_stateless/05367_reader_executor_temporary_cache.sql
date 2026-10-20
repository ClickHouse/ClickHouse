-- Tags: no-fasttest, no-parallel, no-parallel-replicas, no-distributed-cache
-- Tag no-fasttest: needs S3 storage with a filesystem cache.
-- Tag no-parallel: uses the dedicated cache of `04511_reader_executor_disk_cache`.
-- Tag no-parallel-replicas: the replicas, not the initiator, count the bytes in `query_log`.
-- Tag no-distributed-cache: the distributed cache reads around the executor's `DiskCacheProvider`.

-- A temporary read fills the filesystem cache and drops what it filled: the next read goes to the source.

DROP TABLE IF EXISTS t_re_temporary_cache;

CREATE TABLE t_re_temporary_cache (k UInt64, v String)
ENGINE = MergeTree ORDER BY k
SETTINGS storage_policy = 's3_cache_04511', min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_re_temporary_cache SELECT number, toString(number) FROM numbers(200000)
SETTINGS enable_filesystem_cache_on_write_operations = 0;

SET use_reader_executor = 1, remote_filesystem_read_method = 'read', enable_filesystem_cache = 1;
SET read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0, use_uncompressed_cache = 0, use_columns_cache = 0;
SET max_threads = 1;

SELECT count(), sum(k) FROM t_re_temporary_cache
SETTINGS log_comment = '05367_temporary', read_from_filesystem_cache_if_exists_otherwise_cache_temporarily = 1;
SELECT count(), sum(k) FROM t_re_temporary_cache SETTINGS log_comment = '05367_after_temporary';
SELECT count(), sum(k) FROM t_re_temporary_cache SETTINGS log_comment = '05367_warm';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['ReaderExecutorCachePopulateRequests'] > 0 AS populated,
    ProfileEvents['ReaderExecutorBytesFromSource'] > 0 AS from_source
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05367_temporary', '05367_after_temporary', '05367_warm')
ORDER BY event_time_microseconds;

DROP TABLE t_re_temporary_cache;
