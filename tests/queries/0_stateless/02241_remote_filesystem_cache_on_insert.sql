-- Tags: no-parallel, no-fasttest

-- { echo }

DROP TABLE IF EXISTS test;
SYSTEM DROP REMOTE FILESYSTEM CACHE;
CREATE TABLE test (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='s3_cache';
INSERT INTO test SELECT number, toString(number) FROM numbers(100) SETTINGS remote_fs_cache_on_insert=1;

SELECT count() FROM system.remote_filesystem_cache;
SELECT  * FROM test FORMAT Null;
SELECT count() size FROM system.remote_filesystem_cache;
SYSTEM DROP REMOTE FILESYSTEM CACHE;
INSERT INTO test SELECT number, toString(number) FROM numbers(100, 100);
SELECT count() size FROM system.remote_filesystem_cache;
INSERT INTO test SELECT number, toString(number) FROM numbers(100) SETTINGS remote_fs_cache_on_insert=0; -- still writes cache because now config setting is used
SELECT count() size FROM system.remote_filesystem_cache;
