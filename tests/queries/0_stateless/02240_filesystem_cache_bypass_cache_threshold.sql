-- Tags: no-parallel, no-fasttest, no-object-storage, no-random-settings

-- { echo }

SYSTEM DROP FILESYSTEM CACHE;
SET enable_filesystem_cache_on_write_operations=0;

DROP TABLE IF EXISTS test;
CREATE TABLE test (key UInt32, value String)
Engine=MergeTree()
ORDER BY key
SETTINGS min_bytes_for_wide_part = 10485760,
         compress_marks=false,
         compress_primary_key=false,
         serialization_info_version='basic',
         disk = disk(
            type = cache,
            name = '02240_bypass_cache_threshold',
            max_size = '128Mi',
            path = 'filesystem_cache_bypass_cache_threshold/',
            enable_bypass_cache_with_threshold = 1,
            bypass_cache_threshold = 100,
            disk = 's3_disk');

INSERT INTO test SELECT number, toString(number) FROM numbers(100);

SELECT  * FROM test FORMAT Null;
SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache WHERE cache_name = '02240_bypass_cache_threshold' ORDER BY file_segment_range_end, size;
SYSTEM DROP FILESYSTEM CACHE;
SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache WHERE cache_name = '02240_bypass_cache_threshold';
SELECT * FROM test FORMAT Null;
SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache WHERE cache_name = '02240_bypass_cache_threshold';
SYSTEM DROP FILESYSTEM CACHE;
SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache WHERE cache_name = '02240_bypass_cache_threshold';
