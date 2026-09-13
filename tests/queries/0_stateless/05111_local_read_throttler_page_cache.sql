-- Tags: no-object-storage
-- Reads served from the OS page cache produce no block device I/O, so they must not consume
-- the tokens of the local read bandwidth throttler.

DROP TABLE IF EXISTS t_local_read_throttler;

CREATE TABLE t_local_read_throttler (x UInt64, s String)
ENGINE = MergeTree ORDER BY x SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_local_read_throttler SELECT number, toString(number) FROM numbers(1000000);

-- The data has just been written, so it is in the OS page cache; read it once more to also warm
-- up the mark cache, and make sure that nothing but the page cache serves the measured query.
SELECT count() FROM t_local_read_throttler WHERE NOT ignore(*)
SETTINGS local_filesystem_read_method = 'pread_threadpool', min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 0,
    use_uncompressed_cache = 0, use_page_cache_for_local_disks = 0, use_page_cache_for_disks_without_file_cache = 0;

SELECT count() FROM t_local_read_throttler WHERE NOT ignore(*)
SETTINGS local_filesystem_read_method = 'pread_threadpool', min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 0,
    use_uncompressed_cache = 0, use_page_cache_for_local_disks = 0, use_page_cache_for_disks_without_file_cache = 0,
    max_local_read_bandwidth = 1000000000, log_comment = '05111_local_read_throttler_page_cache';

SYSTEM FLUSH LOGS query_log;

-- The throttler must have accounted exactly the bytes that were read from the device.
SELECT
    ProfileEvents['ThreadPoolReaderPageCacheHitBytes'] > 0 AS served_from_page_cache,
    ProfileEvents['QueryLocalReadThrottlerBytes'] = ProfileEvents['ThreadPoolReaderPageCacheMissBytes'] AS throttled_exactly_the_device_reads
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment = '05111_local_read_throttler_page_cache';

DROP TABLE t_local_read_throttler;
