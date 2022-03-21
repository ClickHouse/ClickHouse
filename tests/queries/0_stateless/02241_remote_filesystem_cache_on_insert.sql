-- Tags: no-parallel, no-fasttest, long

-- { echo }

DROP TABLE IF EXISTS test;
SYSTEM DROP REMOTE FILESYSTEM CACHE;
-- CREATE TABLE test (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='s3_cache';
CREATE TABLE test (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='s3';
INSERT INTO test SELECT number, toString(number) FROM numbers(100) SETTINGS remote_fs_cache_on_insert=1;

SELECT cache_base_path, file_segment_range, size FROM system.remote_filesystem_cache;
SELECT  * FROM test FORMAT Null;
SELECT cache_base_path, file_segment_range, size FROM system.remote_filesystem_cache;
SYSTEM DROP REMOTE FILESYSTEM CACHE;
INSERT INTO test SELECT number, toString(number) FROM numbers(100, 100);
SELECT cache_base_path, file_segment_range, size FROM system.remote_filesystem_cache;
