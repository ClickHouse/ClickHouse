-- Tags: no-parallel, no-fasttest, long

SET max_memory_usage='20G';
SET enable_filesystem_cache_on_write_operations = 0;

DROP TABLE IF EXISTS test;
CREATE TABLE test (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='s3_cache';
INSERT INTO test SELECT * FROM generateRandom('key UInt32, value String') LIMIT 10000;

SET remote_filesystem_read_method='threadpool';

SELECT 1, * FROM test LIMIT 10 FORMAT Null;

SYSTEM FLUSH LOGS;
SELECT query,
       ProfileEvents['CachedReadBufferReadFromSourceBytes'] > 0 as remote_fs_read,
       ProfileEvents['CachedReadBufferReadFromCacheBytes'] > 0 as remote_fs_cache_read,
       ProfileEvents['CachedReadBufferCacheWriteBytes'] > 0 as remote_fs_read_and_download
FROM system.query_log
WHERE query LIKE 'SELECT 1, * FROM test LIMIT%'
AND type = 'QueryFinish'
AND current_database = currentDatabase()
ORDER BY query_start_time DESC
LIMIT 1;

SET remote_filesystem_read_method='read';

SELECT 2, * FROM test LIMIT 10 FORMAT Null;

SYSTEM FLUSH LOGS;
SELECT query,
       ProfileEvents['CachedReadBufferReadFromSourceBytes'] > 0 as remote_fs_read,
       ProfileEvents['CachedReadBufferReadFromCacheBytes'] > 0 as remote_fs_cache_read,
       ProfileEvents['CachedReadBufferCacheWriteBytes'] > 0 as remote_fs_read_and_download
FROM system.query_log
WHERE query LIKE 'SELECT 2, * FROM test LIMIT%'
AND type = 'QueryFinish'
AND current_database = currentDatabase()
ORDER BY query_start_time DESC
LIMIT 1;

SET remote_filesystem_read_method='threadpool';

SELECT * FROM test WHERE value LIKE '%abc%' ORDER BY value LIMIT 10 FORMAT Null;

SET enable_filesystem_cache_on_write_operations = 1;

TRUNCATE TABLE test;
SELECT count() FROM test;

SYSTEM DROP FILESYSTEM CACHE;

INSERT INTO test SELECT * FROM generateRandom('key UInt32, value String') LIMIT 10000;

SELECT 3, * FROM test LIMIT 10 FORMAT Null;

SYSTEM FLUSH LOGS;
SELECT query,
       ProfileEvents['CachedReadBufferReadFromSourceBytes'] > 0 as remote_fs_read,
       ProfileEvents['CachedReadBufferReadFromCacheBytes'] > 0 as remote_fs_cache_read,
       ProfileEvents['CachedReadBufferCacheWriteBytes'] > 0 as remote_fs_read_and_download
FROM system.query_log
WHERE query LIKE 'SELECT 3, * FROM test LIMIT%'
AND type = 'QueryFinish'
AND current_database = currentDatabase()
ORDER BY query_start_time DESC
LIMIT 1;

DROP TABLE test;
