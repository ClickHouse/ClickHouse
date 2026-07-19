-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- On a hit of the Planner-level (subquery) query result cache no plan is built for the subquery,
-- so the `databases`, `tables` and `columns` fields of `system.query_log` used to lose the tables
-- read by the subquery. The access info is now reconstructed from the analyzed query tree.

SET enable_analyzer = 1;

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_subquery_access;
DROP TABLE IF EXISTS t_subquery_access_joined;

CREATE TABLE t_subquery_access (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_subquery_access VALUES (1), (2), (3);

CREATE TABLE t_subquery_access_joined (y UInt64) ENGINE = MergeTree ORDER BY y;
INSERT INTO t_subquery_access_joined VALUES (1), (2);

-- The first run computes the subquery and stores its result in the query cache
SELECT sum(x) FROM (SELECT x FROM t_subquery_access SETTINGS use_query_cache = true);

SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;

-- The second run is answered from the query cache
SELECT sum(x) FROM (SELECT x FROM t_subquery_access SETTINGS use_query_cache = true);

SYSTEM FLUSH LOGS query_log;

-- Both runs must report the database, table and column read by the subquery
SELECT ProfileEvents['QueryCacheHits'] AS hits,
       has(databases, currentDatabase()) AS database_ok,
       has(tables, currentDatabase() || '.t_subquery_access') AS table_ok,
       has(columns, currentDatabase() || '.t_subquery_access.x') AS column_ok
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE 'SELECT sum(x) FROM (SELECT x FROM t_subquery_access%'
ORDER BY event_time_microseconds;

-- The same for a cached subquery which reads multiple tables, one of them inside a nested subquery
SELECT count() FROM (SELECT x FROM t_subquery_access JOIN (SELECT y FROM t_subquery_access_joined) AS js ON x = js.y SETTINGS use_query_cache = true);
SELECT count() FROM (SELECT x FROM t_subquery_access JOIN (SELECT y FROM t_subquery_access_joined) AS js ON x = js.y SETTINGS use_query_cache = true);

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['QueryCacheHits'] AS hits,
       has(tables, currentDatabase() || '.t_subquery_access') AS table_ok,
       has(tables, currentDatabase() || '.t_subquery_access_joined') AS joined_table_ok
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND query LIKE 'SELECT count() FROM (SELECT x FROM t_subquery_access JOIN%'
ORDER BY event_time_microseconds;

SYSTEM DROP QUERY CACHE;

DROP TABLE t_subquery_access;
DROP TABLE t_subquery_access_joined;
