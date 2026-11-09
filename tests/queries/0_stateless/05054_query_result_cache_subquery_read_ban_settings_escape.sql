-- Tags: no-parallel
-- - no-parallel - the query result cache is shared

-- The read path of the Planner-level query result cache is banned while a distributed plan is being
-- built: a hit replaces the whole subquery plan with a non-serializable `ReadFromQueryResultCacheStep`,
-- so the outer plan's shape would depend on cache contents. The ban must not be escapable from inside
-- the query: a subquery that specifies `SETTINGS make_distributed_plan = 0` locally is still part of
-- the outer distributed plan, so its cache read stays banned (`building_distributed_plan` in
-- `SelectQueryOptions` is sticky).

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_qrc_read_escape;
CREATE TABLE t_qrc_read_escape (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_qrc_read_escape SELECT number FROM numbers(10);

-- Subquery caching is a Planner feature: with the old analyzer no subquery cache entry is created at
-- all. Pin the analyzer, because some CI jobs run the whole suite with `enable_analyzer = 0`.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_reads_from_query_cache = 1;
-- Set session-wide (it is inert without `make_distributed_plan`), so the subquery's effective
-- settings - and therefore its cache key - are identical in the two runs below.
SET distributed_plan_execute_locally = 1;

SELECT '-- warm the cache with the same subquery, planned without a distributed plan';
SELECT k FROM (SELECT k FROM t_qrc_read_escape WHERE k = 4 SETTINGS use_query_cache = 1, make_distributed_plan = 0);
SELECT count() FROM system.query_cache WHERE is_subquery = 1 AND query LIKE '%' || currentDatabase() || '.t_qrc_read_escape WHERE k = 4%';

SELECT '-- the distributed outer query must not read the entry despite the local setting override';
SELECT k FROM (SELECT k FROM t_qrc_read_escape WHERE k = 4 SETTINGS use_query_cache = 1, make_distributed_plan = 0)
    SETTINGS make_distributed_plan = 1;

SYSTEM FLUSH LOGS query_log;
-- No cache probe may happen during distributed planning: neither a hit nor a miss.
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%t_qrc_read_escape%'
    AND query LIKE '%SETTINGS make_distributed_plan = 1%'
    AND query NOT LIKE '%query_log%';

DROP TABLE t_qrc_read_escape;
SYSTEM DROP QUERY CACHE;
