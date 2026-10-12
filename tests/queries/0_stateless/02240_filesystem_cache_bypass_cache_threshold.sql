-- Tags: no-fasttest, no-object-storage, no-random-settings
-- The subject of this test is that a read larger than `bypass_cache_threshold` is not cached.
-- The first read also caches one sub-threshold segment (80 bytes, the marks of the compact part),
-- which is the other half of the same behaviour and is asserted below.
-- The assertion after the *second* read must not depend on whether that sub-threshold segment
-- reappears: it only stays absent while the marks are still resident in the process-wide mark
-- cache, and concurrently running tests evict them, after which the marks are re-read from disk
-- and cached again. So that one assertion is restricted to above-threshold segments, which is
-- what the test is actually about.
-- These lines must stay directly below the `Tags:` line with no blank line in between:
-- `getTestTagsLength` only strips a contiguous run of comments starting there, and anything it
-- does not strip is echoed by `-- { echo }` into the output that is compared to the reference.

-- { echo }

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
SYSTEM CLEAR FILESYSTEM CACHE '02240_bypass_cache_threshold';
SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache WHERE cache_name = '02240_bypass_cache_threshold';
SELECT * FROM test FORMAT Null;
SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache WHERE cache_name = '02240_bypass_cache_threshold' AND size > 100;
SYSTEM CLEAR FILESYSTEM CACHE '02240_bypass_cache_threshold';
SELECT file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache WHERE cache_name = '02240_bypass_cache_threshold';
