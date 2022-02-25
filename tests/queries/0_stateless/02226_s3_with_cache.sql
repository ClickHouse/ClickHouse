-- Tags: no-parallel, no-fasttest

CREATE TABLE test (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='s3_cache';
INSERT INTO test SELECT * FROM generateRandom('key UInt32, value String') LIMIT 100000000;

SET remote_filesystem_read_method='threadpool';

SELECT 1, * FROM test LIMIT 10 FORMAT Null;

SYSTEM FLUSH LOGS;
SELECT query,
       ProfileEvents['RemoteFSReadBytes'] as remote_fs_read,
       ProfileEvents['RemoteFSCacheReadBytes'] as remote_fs_cache_read,
       ProfileEvents['RemoteFSCacheDownloadedBytes'] as remote_fs_read_and_download
FROM system.query_log
WHERE query LIKE 'SELECT 1, * FROM test LIMIT%'
AND type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 1;

SELECT * FROM test WHERE value LIKE '%abc%' ORDER BY value LIMIT 10 FORMAT Null;
SELECT * FROM test ORDER BY value LIMIT 10 FORMAT Null;
SELECT * FROM test WHERE value LIKE '%dba%' ORDER BY value LIMIT 10 FORMAT Null;

SET remote_filesystem_read_method='read';

SELECT 2, * FROM test LIMIT 10 FORMAT Null;

SYSTEM FLUSH LOGS;
SELECT query,
       ProfileEvents['RemoteFSReadBytes'] as remote_fs_read,
       ProfileEvents['RemoteFSCacheReadBytes'] as remote_fs_cache_read,
       ProfileEvents['RemoteFSCacheDownloadedBytes'] as remote_fs_read_and_download
FROM system.query_log
WHERE query LIKE 'SELECT 2, * FROM test LIMIT%'
AND type = 'QueryFinish'
ORDER BY query_start_time DESC
LIMIT 1;
