-- Tags: no-parallel, no-fasttest

-- { echo }

SET remote_fs_cache_on_insert=1;

DROP TABLE IF EXISTS test;
CREATE TABLE test (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='s3_cache';

SYSTEM DROP REMOTE FILESYSTEM CACHE;

SELECT file_segment_range, size, state FROM (SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path FROM system.remote_data_paths ) AS data_paths INNER JOIN system.remote_filesystem_cache AS caches ON data_paths.cache_path = caches.cache_path FORMAT Vertical;
SELECT file_segment_range, size, state FROM system.remote_filesystem_cache format Vertical;

INSERT INTO test SELECT number, toString(number) FROM numbers(100);

SELECT file_segment_range, size, state FROM (SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path FROM system.remote_data_paths ) AS data_paths INNER JOIN system.remote_filesystem_cache AS caches ON data_paths.cache_path = caches.cache_path FORMAT Vertical;
SELECT file_segment_range, size, state FROM system.remote_filesystem_cache format Vertical;

SELECT cache_hits FROM system.remote_filesystem_cache;

SELECT  * FROM test FORMAT Null;
SELECT cache_hits FROM system.remote_filesystem_cache;

SELECT  * FROM test FORMAT Null;
SELECT cache_hits FROM system.remote_filesystem_cache;

SELECT count() size FROM system.remote_filesystem_cache;

SYSTEM DROP REMOTE FILESYSTEM CACHE;

INSERT INTO test SELECT number, toString(number) FROM numbers(100, 200);

SELECT file_segment_range, size, state FROM (SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path FROM system.remote_data_paths ) AS data_paths INNER JOIN system.remote_filesystem_cache AS caches ON data_paths.cache_path = caches.cache_path ORDER BY size FORMAT Vertical;
SELECT file_segment_range, size, state FROM system.remote_filesystem_cache format Vertical;

-- INSERT INTO test SELECT number, toString(number) FROM numbers(100) SETTINGS remote_fs_cache_on_insert=0; -- still writes cache because now config setting is used
