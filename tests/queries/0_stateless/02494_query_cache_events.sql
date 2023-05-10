-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Start with empty query cache QC and query log
SYSTEM DROP QUERY CACHE;
DROP TABLE system.query_log SYNC;

-- Run a query with QC on. The first execution is a QC miss.
SELECT '---';
SELECT 1 SETTINGS use_query_cache = true;

SYSTEM FLUSH LOGS;
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query = 'SELECT 1 SETTINGS use_query_cache = true;';


-- Run previous query again with query cache on
SELECT '---';
SELECT 1 SETTINGS use_query_cache = true;

DROP TABLE system.query_log SYNC;
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query = 'SELECT 1 SETTINGS use_query_cache = true;';

SYSTEM DROP QUERY CACHE;
