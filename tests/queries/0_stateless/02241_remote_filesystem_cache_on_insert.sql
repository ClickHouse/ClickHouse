-- Tags: no-parallel, no-fasttest, no-s3-storage

-- { echo }

SET enable_filesystem_cache_on_write_operations=1;

DROP TABLE IF EXISTS test;
CREATE TABLE test (key UInt32, value String) Engine=MergeTree() ORDER BY key SETTINGS storage_policy='s3_cache', min_bytes_for_wide_part = 10485760;
SYSTEM STOP MERGES test;
SYSTEM DROP FILESYSTEM CACHE;
SELECT file_segment_range_begin, file_segment_range_end, size, state
FROM
(
    SELECT file_segment_range_begin, file_segment_range_end, size, state, local_path
    FROM
    (
        SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path
        FROM system.remote_data_paths
    ) AS data_paths
    INNER JOIN
        system.filesystem_cache AS caches
    ON data_paths.cache_path = caches.cache_path
)
WHERE endsWith(local_path, 'data.bin')
FORMAT Vertical;

SELECT count() FROM (SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path FROM system.remote_data_paths ) AS data_paths INNER JOIN system.filesystem_cache AS caches ON data_paths.cache_path = caches.cache_path;
SELECT count() FROM system.filesystem_cache;

INSERT INTO test SELECT number, toString(number) FROM numbers(100);

SELECT file_segment_range_begin, file_segment_range_end, size, state
FROM
(
    SELECT file_segment_range_begin, file_segment_range_end, size, state, local_path
    FROM
    (
        SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path
        FROM system.remote_data_paths
    ) AS data_paths
    INNER JOIN
        system.filesystem_cache AS caches
    ON data_paths.cache_path = caches.cache_path
)
WHERE endsWith(local_path, 'data.bin')
FORMAT Vertical;

SELECT count() FROM (SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path FROM system.remote_data_paths ) AS data_paths INNER JOIN system.filesystem_cache AS caches ON data_paths.cache_path = caches.cache_path;
SELECT count() FROM system.filesystem_cache;

SELECT count() FROM system.filesystem_cache WHERE cache_hits > 0;

SELECT  * FROM test FORMAT Null;
SELECT count() FROM system.filesystem_cache WHERE cache_hits > 0;

SELECT  * FROM test FORMAT Null;
SELECT count() FROM system.filesystem_cache WHERE cache_hits > 0;

SELECT count() size FROM system.filesystem_cache;

SYSTEM DROP FILESYSTEM CACHE;

INSERT INTO test SELECT number, toString(number) FROM numbers(100, 200);

SELECT file_segment_range_begin, file_segment_range_end, size, state
FROM
(
    SELECT file_segment_range_begin, file_segment_range_end, size, state, local_path
    FROM
    (
        SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path
        FROM system.remote_data_paths
    ) AS data_paths
    INNER JOIN
        system.filesystem_cache AS caches
    ON data_paths.cache_path = caches.cache_path
)
WHERE endsWith(local_path, 'data.bin')
FORMAT Vertical;

SELECT count() FROM (SELECT arrayJoin(cache_paths) AS cache_path, local_path, remote_path FROM system.remote_data_paths ) AS data_paths INNER JOIN system.filesystem_cache AS caches ON data_paths.cache_path = caches.cache_path;
SELECT count() FROM system.filesystem_cache;

SELECT count() FROM system.filesystem_cache;
INSERT INTO test SELECT number, toString(number) FROM numbers(100) SETTINGS enable_filesystem_cache_on_write_operations=0;
SELECT count() FROM system.filesystem_cache;

INSERT INTO test SELECT number, toString(number) FROM numbers(100);
INSERT INTO test SELECT number, toString(number) FROM numbers(300, 10000);
SELECT count() FROM system.filesystem_cache;

SYSTEM START MERGES test;

OPTIMIZE TABLE test FINAL;
SELECT count() FROM system.filesystem_cache;

SET mutations_sync=2;
ALTER TABLE test UPDATE value = 'kek' WHERE key = 100;
SELECT count() FROM system.filesystem_cache;

INSERT INTO test SELECT number, toString(number) FROM numbers(5000000);
SYSTEM FLUSH LOGS;
SELECT
    query, ProfileEvents['RemoteFSReadBytes'] > 0 as remote_fs_read
FROM
    system.query_log
WHERE
    query LIKE 'SELECT number, toString(number) FROM numbers(5000000)%'
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
ORDER BY
    query_start_time
    DESC
LIMIT 1;

SELECT count() FROM test;
SELECT count() FROM test WHERE value LIKE '%010%';
