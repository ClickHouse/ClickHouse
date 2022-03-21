-- Tags: no-parallel, no-fasttest

-- { echo }

SYSTEM DROP REMOTE FILESYSTEM CACHE;
CREATE TABLE test (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='s3_cache';
INSERT INTO test SELECT number, toString(number) FROM numbers(100);
DROP TABLE IF EXISTS test;

SELECT  * FROM test FORMAT Null;
SELECT cache_base_path, file_segment_range, size FROM system.remote_filesystem_cache;
SYSTEM DROP REMOTE FILESYSTEM CACHE;
SELECT cache_base_path, file_segment_range, size FROM system.remote_filesystem_cache;
SELECT * FROM test FORMAT Null;
SELECT cache_base_path, file_segment_range, size FROM system.remote_filesystem_cache;
SYSTEM DROP REMOTE FILESYSTEM CACHE;
SELECT cache_base_path, file_segment_range, size FROM system.remote_filesystem_cache;
