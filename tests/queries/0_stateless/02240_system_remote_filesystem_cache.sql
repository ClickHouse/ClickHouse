-- Tags: no-parallel, no-fasttest

-- { echo }

SYSTEM DROP REMOTE FILESYSTEM CACHE;
SET remote_fs_cache_on_write_operations=0;
DROP TABLE IF EXISTS test;
CREATE TABLE test (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='s3';
INSERT INTO test SELECT number, toString(number) FROM numbers(100);

SELECT  * FROM test FORMAT Null;
SELECT cache_base_path, file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache ORDER BY file_segment_range_end, size;
SYSTEM DROP REMOTE FILESYSTEM CACHE;
SELECT cache_base_path, file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache;
SELECT * FROM test FORMAT Null;
SELECT cache_base_path, file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache;
SYSTEM DROP REMOTE FILESYSTEM CACHE;
SELECT cache_base_path, file_segment_range_begin, file_segment_range_end, size FROM system.filesystem_cache;
