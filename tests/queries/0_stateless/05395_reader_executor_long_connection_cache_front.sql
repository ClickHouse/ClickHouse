-- Tags: no-fasttest
-- A held GET of the `ReaderExecutor` that starts at the committed front of a filesystem cache segment must
-- reach the read position. It was sized from the position but opened at the fetch start, so it ended short:
-- `ReaderExecutor: source ended` (CANNOT_READ_ALL_DATA).
-- https://github.com/ClickHouse/ClickHouse/pull/125606#issuecomment-6196861242

DROP TABLE IF EXISTS t_reader_executor_cache_front;

-- The layout is pinned: random granularity and compression block sizes move the ranges and can hide the bug.
CREATE TABLE t_reader_executor_cache_front (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS storage_policy = 's3_cache', min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi',
    min_compress_block_size = 65536, max_compress_block_size = 1048576;

-- No write-through, so the reads below start with a cold cache and go to the source.
INSERT INTO t_reader_executor_cache_front SELECT number, cityHash64(number) FROM numbers(1000000)
SETTINGS enable_filesystem_cache_on_write_operations = 0;

SET use_reader_executor = 1, reader_executor_use_long_connections = 1, reader_executor_window_size = 131072;
-- One thread reads through the filesystem cache, with no other cache or prefetch in front of it.
SET enable_filesystem_cache = 1, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0, remote_filesystem_read_method = 'read',
    remote_filesystem_read_prefetch = 0, allow_prefetched_read_pool_for_remote_filesystem = 0, max_threads = 1,
    use_page_cache_for_object_storage = 0, use_uncompressed_cache = 0, use_columns_cache = 0;

-- Cold, then warm.
SELECT sum(v) FROM t_reader_executor_cache_front WHERE k < 20000 OR k > 900000;
SELECT sum(v) FROM t_reader_executor_cache_front WHERE k < 20000 OR k > 900000;

DROP TABLE t_reader_executor_cache_front;
