-- The files of the filesystem cache are read with `pread` regardless of `local_filesystem_read_method`.
-- When they are served from the OS page cache, they produce no block device I/O, so they must not consume
-- the tokens of the local read bandwidth throttler.

DROP TABLE IF EXISTS t_local_read_throttler_fs_cache;

CREATE TABLE t_local_read_throttler_fs_cache (x UInt64, s String)
ENGINE = MergeTree ORDER BY x SETTINGS disk = 'local_cache', min_bytes_for_wide_part = 0;

INSERT INTO t_local_read_throttler_fs_cache SELECT number, toString(number) FROM numbers(1000000);

-- Download the data into the filesystem cache, so that the cache files are in the OS page cache.
SELECT count() FROM t_local_read_throttler_fs_cache WHERE NOT ignore(*)
SETTINGS enable_filesystem_cache = 1, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0,
    min_bytes_to_use_direct_io = 0, use_uncompressed_cache = 0, use_page_cache_for_disks_without_file_cache = 0, use_columns_cache = 0;

-- `enable_parallel_replicas = 0`: with parallel replicas another replica may do the read,
-- and its `ProfileEvents` never reach this query's `query_log` row.
SELECT count() FROM t_local_read_throttler_fs_cache WHERE NOT ignore(*)
SETTINGS enable_filesystem_cache = 1, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0,
    min_bytes_to_use_direct_io = 0, use_uncompressed_cache = 0, use_page_cache_for_disks_without_file_cache = 0, use_columns_cache = 0,
    max_local_read_bandwidth = 1000000000, enable_parallel_replicas = 0, log_comment = '05317_local_read_throttler_filesystem_cache';

SYSTEM FLUSH LOGS query_log;

-- The throttler must account nothing but the reads that were not OS page cache hits, see
-- `05111_local_read_throttler_page_cache`. Whether they were page cache hits at all is not checked:
-- `preadv2` with `RWF_NOWAIT` is not usable on every system, and then the check below holds trivially.
SELECT
    ProfileEvents['CachedReadBufferReadFromCacheBytes'] > 0 AS read_from_filesystem_cache,
    ProfileEvents['QueryLocalReadThrottlerBytes']
        <= ProfileEvents['ReadBufferFromFileDescriptorReadBytes']
            - ProfileEvents['ThreadPoolReaderPageCacheHitBytes']
            - ProfileEvents['ReadBufferFromFileDescriptorPageCacheHitBytes'] AS page_cache_hits_not_throttled
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment = '05317_local_read_throttler_filesystem_cache';

DROP TABLE t_local_read_throttler_fs_cache;
