-- Tags: no-parallel, no-fasttest, no-object-storage, no-random-settings

-- { echo }

SYSTEM DROP FILESYSTEM CACHE;
SET enable_filesystem_cache_on_write_operations=0;
SET skip_download_if_exceeds_query_cache=1;
SET filesystem_cache_max_download_size=128;

DROP TABLE IF EXISTS test;
CREATE TABLE test (key UInt32, value String)
Engine=MergeTree()
ORDER BY key
SETTINGS min_bytes_for_wide_part = 10485760,
         compress_marks=false,
         compress_primary_key=false,
         disk = disk(
            type = cache,
            max_size = '128Mi',
            path = 'filesystem_query_cache/',
            cache_on_write_operations= 1,
            enable_filesystem_query_cache_limit = 1,
            delayed_cleanup_interval_ms = 100,
            disk = 's3_disk');
SYSTEM DROP FILESYSTEM CACHE;
INSERT INTO test SELECT number, toString(number) FROM numbers(100);
SELECT  * FROM test FORMAT Null;
SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache ORDER BY file_segment_range_end, size;
SYSTEM DROP FILESYSTEM CACHE;
SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache;
SELECT * FROM test FORMAT Null;
SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache;
SYSTEM DROP FILESYSTEM CACHE;
SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache;
