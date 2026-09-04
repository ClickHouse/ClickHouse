
-- Start with empty query cache (QC).
SET query_cache_tag = '02494_query_cache_passive_usage';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_passive_usage';

-- By default, don't write query result into QC.
SELECT 1;
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_passive_usage') AS test_query_cache;

SELECT '-----';

-- Try to retrieve query from empty QC using the passive mode. Do this by disabling the active mode. The cache should still be empty (no insert).
SELECT 1 SETTINGS use_query_cache = true, enable_writes_to_query_cache = false;
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_passive_usage') AS test_query_cache;

SELECT '-----';

-- Put query into cache.
SELECT 1 SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_passive_usage') AS test_query_cache;

SELECT '-----';

/* Run same query with passive mode again. There must still be one entry in the QC and we must have a QC hit. */

SELECT 1 SETTINGS use_query_cache = true, enable_writes_to_query_cache = false;
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_passive_usage') AS test_query_cache;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  /* NOTE: client incorrectly join comments from the previous line into query, hence LIKE */
  AND query LIKE '%\nSELECT 1 SETTINGS use_query_cache = true, enable_writes_to_query_cache = false;'
ORDER BY event_time_microseconds;

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_passive_usage';
