-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Start with empty query cache (QC).
SYSTEM DROP QUERY CACHE;

-- By default, don't write query result into QC.
SELECT 1;
SELECT COUNT(*) FROM system.query_cache;

SELECT '-----';

-- Try to retrieve query from empty QC using the passive mode. Do this by disabling the active mode. The cache should still be empty (no insert).
SELECT 1 SETTINGS use_query_cache = true, enable_writes_to_query_cache = false;
SELECT COUNT(*) FROM system.query_cache;

SELECT '-----';

-- Put query into cache.
SELECT 1 SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;

SELECT '-----';

-- Run same query with passive mode again. There must still be one entry in the QC and we must have a QC hit.

-- Get rid of log of previous SELECT
DROP TABLE system.query_log SYNC;

SELECT 1 SETTINGS use_query_cache = true, enable_writes_to_query_cache = false;
SELECT COUNT(*) FROM system.query_cache;

SYSTEM FLUSH LOGS;
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND query = 'SELECT 1 SETTINGS use_query_cache = true, enable_writes_to_query_cache = false;';

SYSTEM DROP QUERY CACHE;
