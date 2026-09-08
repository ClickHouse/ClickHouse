-- Tags: no-parallel
-- Tag no-parallel: drops the server-wide query plan cache and inspects system.query_log

-- Settings that do not affect the query plan are stripped from the AST before the cache key is
-- computed. Once every entry of a `SETTINGS` clause has been stripped, the clause itself must be
-- dropped too, so that `SELECT ... SETTINGS log_comment = '...'` shares its cache entry with the
-- same query without a `SETTINGS` clause.

SET enable_query_plan_cache = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_qpc_ignored_settings;
CREATE TABLE t_qpc_ignored_settings (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_qpc_ignored_settings VALUES (1), (2);

DROP TABLE IF EXISTS t_qpc_ignored_settings_start;
CREATE TABLE t_qpc_ignored_settings_start (ts DateTime64(6)) ENGINE = Memory;
INSERT INTO t_qpc_ignored_settings_start VALUES (now64(6));

SYSTEM DROP QUERY PLAN CACHE;

-- The first query has no `SETTINGS` clause at all, the second one has a clause holding only an
-- ignored setting. The second query must hit the entry inserted by the first one.
SELECT a FROM t_qpc_ignored_settings ORDER BY a;
SELECT a FROM t_qpc_ignored_settings ORDER BY a SETTINGS log_comment = 'qpc_ignored_settings_hit';

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['QueryPlanCacheHits'],
    ProfileEvents['QueryPlanCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND event_time_microseconds >= (SELECT ts FROM t_qpc_ignored_settings_start)
  AND log_comment = 'qpc_ignored_settings_hit';

DROP TABLE t_qpc_ignored_settings;
DROP TABLE t_qpc_ignored_settings_start;
