-- Tags: no-fasttest, no-random-settings
-- no-fasttest -- requires Parquet and S3
-- no-random-settings -- the filesystem cache settings and the readBigAt path must not be randomized

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/97325 , fixed in
-- https://github.com/ClickHouse/ClickHouse/pull/97890 .
--
-- `CachedOnDiskReadBufferFromFile::readBigAt` computed a resumed offset from the stale
-- `file_offset_of_buffer_end` member instead of the per-call `offset` argument, once a read
-- spanned a filesystem-cache-segment boundary while the read type transitioned between
-- `CACHED` and `REMOTE_FS_READ_AND_PUT_IN_CACHE`. That produced a wrong seek position and
-- `CANNOT_SEEK_THROUGH_FILE`.
--
-- To exercise this deterministically (without the 14 GiB public `hits.parquet`), write a small
-- synthetic Parquet file whose column data is several times bigger than the 1 MiB
-- `max_file_segment_size`/`boundary_alignment` of the `cache_for_readbigat` cache (defined in
-- tests/config/config.d/storage_conf.xml), split over several row groups, then read it back
-- twice through that cache: once cold (forces `REMOTE_FS_READ_AND_PUT_IN_CACHE`, i.e. the
-- boundary-crossing "producer" side) and once warm (forces `CACHED`, i.e. the boundary-crossing
-- "consumer" side), matching the mixed-state scenario of the original bug.

DROP TABLE IF EXISTS t_cached_read_big_at;

CREATE TABLE t_cached_read_big_at (id UInt64, s String)
ENGINE = S3(s3_conn, filename = '03988_cached_read_big_at.parquet', format = 'Parquet');

-- 100000 rows x 128-byte unique strings ~= 12.8 MiB of column data, split into 5 row groups
-- (~2.56 MiB each), i.e. every row group has a string column chunk spanning several 1 MiB
-- filesystem cache segments. Compression is disabled so the on-disk size is a simple, exactly
-- derivable function of the row count and string length (no reliance on codec-specific ratios).
INSERT INTO t_cached_read_big_at
SELECT number, repeat(hex(MD5(toString(number))), 4)
FROM numbers(100000)
SETTINGS
    s3_truncate_on_insert = 1,
    output_format_parquet_row_group_size = 20000,
    output_format_parquet_compression_method = 'none';

SYSTEM DROP FILESYSTEM CACHE 'cache_for_readbigat';

-- Cold read: nothing is cached yet, so every file segment goes through
-- REMOTE_FS_READ_AND_PUT_IN_CACHE.
SELECT count(), sum(id), sum(length(s))
FROM t_cached_read_big_at
SETTINGS
    filesystem_cache_name = 'cache_for_readbigat',
    enable_filesystem_cache = 1,
    max_download_threads = 1,
    use_parquet_metadata_cache = 0,
    log_comment = '03988_cached_read_big_at_cold';

-- Warm read: everything from the previous read is now in the cache, so every file segment goes
-- through CACHED.
SELECT count(), sum(id), sum(length(s))
FROM t_cached_read_big_at
SETTINGS
    filesystem_cache_name = 'cache_for_readbigat',
    enable_filesystem_cache = 1,
    max_download_threads = 1,
    use_parquet_metadata_cache = 0,
    log_comment = '03988_cached_read_big_at_warm';

SYSTEM FLUSH LOGS query_log;

-- The cold read must have actually gone through the readBigAt path (ParquetPrefetcherReadRandomRead)
-- and pulled more than one 1 MiB filesystem cache segment from the source (REMOTE_FS_READ_AND_PUT_IN_CACHE).
SELECT
    ProfileEvents['ParquetPrefetcherReadRandomRead'] > 0,
    ProfileEvents['CachedReadBufferReadFromSourceBytes'] > 1048576
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
    AND query_kind = 'Select' AND current_database = currentDatabase()
    AND log_comment = '03988_cached_read_big_at_cold'
ORDER BY event_time DESC
LIMIT 1;

-- The warm read must have actually gone through the readBigAt path and pulled more than one
-- 1 MiB filesystem cache segment from the cache itself (CACHED).
SELECT
    ProfileEvents['ParquetPrefetcherReadRandomRead'] > 0,
    ProfileEvents['CachedReadBufferReadFromCacheBytes'] > 1048576
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
    AND query_kind = 'Select' AND current_database = currentDatabase()
    AND log_comment = '03988_cached_read_big_at_warm'
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE IF EXISTS t_cached_read_big_at;
