-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;
DROP TABLE IF EXISTS t;

CREATE TABLE t(id Int64, c String) ENGINE=MergeTree ORDER BY id;
INSERT INTO t SELECT number, concat('abc_', number) FROM numbers(10);

SELECT '-- first run';
SELECT * FROM t ORDER BY id
SETTINGS use_query_cache = true,enable_writes_to_query_cache_disk=true,enable_reads_from_query_cache_disk=true;

SELECT '-- profile event: QueryCacheDiskHits, QueryCacheDiskMisses';
SYSTEM FLUSH LOGS;
SELECT
    ProfileEvents['QueryCacheDiskHits'],
    ProfileEvents['QueryCacheDiskMisses']
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT * FROM t ORDER BY id
SETTINGS use_query_cache = true,enable_writes_to_query_cache_disk=true,enable_reads_from_query_cache_disk=true;'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT 'clear memory cache';
SYSTEM DROP QUERY CACHE TYPE 'Memory';
SELECT query,result_size,type FROM system.query_cache;

SELECT '-- read from disk cache';
SELECT * FROM t ORDER BY id
SETTINGS use_query_cache = true,enable_writes_to_query_cache_disk=true,enable_reads_from_query_cache_disk=true;

SELECT '-- profile event: QueryCacheDiskHits, QueryCacheDiskMisses';
SYSTEM FLUSH LOGS;
SELECT
    ProfileEvents['QueryCacheDiskHits'],
    ProfileEvents['QueryCacheDiskMisses']
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query = 'SELECT * FROM t ORDER BY id
SETTINGS use_query_cache = true,enable_writes_to_query_cache_disk=true,enable_reads_from_query_cache_disk=true;'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t;
SYSTEM DROP QUERY CACHE;
