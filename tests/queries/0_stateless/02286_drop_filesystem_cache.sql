-- Tags: no-parallel, no-fasttest, no-s3-storage

-- { echo }

SET enable_filesystem_cache_on_write_operations=0;

DROP TABLE IF EXISTS test;
CREATE TABLE test (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='s3_cache', min_bytes_for_wide_part = 10485760;

SYSTEM DROP FILESYSTEM CACHE;

SELECT count() FROM system.filesystem_cache;
INSERT INTO test SELECT number, toString(number) FROM numbers(100);

SELECT * FROM test FORMAT Null;
SELECT count() FROM system.filesystem_cache;

SYSTEM DROP FILESYSTEM CACHE;
SELECT count() FROM system.filesystem_cache;

SELECT * FROM test FORMAT Null;
SELECT count() FROM system.filesystem_cache;

SYSTEM DROP FILESYSTEM CACHE './data'; -- { serverError 36 }
SELECT count() FROM system.filesystem_cache;

DROP TABLE IF EXISTS test2;
CREATE TABLE test2 (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='s3_cache_2', min_bytes_for_wide_part = 10485760;
INSERT INTO test2 SELECT number, toString(number) FROM numbers(100);
SELECT * FROM test2 FORMAT Null;
SELECT count() FROM system.filesystem_cache;

SYSTEM DROP FILESYSTEM CACHE './s3_cache/';
SELECT count() FROM system.filesystem_cache;

SYSTEM DROP FILESYSTEM CACHE ON CLUSTER;
