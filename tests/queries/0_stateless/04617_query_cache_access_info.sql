-- Tags: no-parallel
-- Tag no-parallel: messes with the internal query cache

-- Test for issue #109027: the `databases` and `tables` columns of `system.query_log`
-- were empty for queries answered from the query cache, because no interpreter runs
-- on a cache hit. The access info is now stored in the cache entry.

SYSTEM CLEAR QUERY CACHE;

DROP TABLE IF EXISTS t_04617;
CREATE TABLE t_04617 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_04617 VALUES (1), (2), (3);

SELECT sum(x) FROM t_04617 SETTINGS use_query_cache = true;
SELECT sum(x) FROM t_04617 SETTINGS use_query_cache = true;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryCacheHits'] AS hits,
       databases = [currentDatabase()] AS databases_ok,
       tables = [currentDatabase() || '.t_04617'] AS tables_ok
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query = 'SELECT sum(x) FROM t_04617 SETTINGS use_query_cache = true;'
ORDER BY event_time_microseconds;

SYSTEM CLEAR QUERY CACHE;
DROP TABLE t_04617;
