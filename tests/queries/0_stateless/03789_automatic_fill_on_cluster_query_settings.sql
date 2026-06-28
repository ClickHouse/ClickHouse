-- Tags: no-parallel, no-replicated-database
-- no-replicated-database: auto-fill skips Replicated databases by design, so the
-- ON CLUSTER assertions only hold under non-replicated database engines.

-- The automatic ON CLUSTER fill decision must respect the query's own SETTINGS clause:
-- the feature has to be switchable per query, overriding the profile/session value. This
-- only holds when the decision is made after the query-level SETTINGS are applied.

SET distributed_ddl_output_mode = 'none';

SELECT 'Test 1: query-level SETTINGS enable auto-fill while the profile setting is off';
-- The profile/session has the feature disabled, but the single query turns it on. With the
-- decision taken before applying query SETTINGS, the statement would stay local; with the fix
-- it is rewritten ON CLUSTER.
SET allow_experimental_automatic_fill_on_cluster_mode = false;
SET cluster_for_automatic_fill_mode = '';
CREATE TABLE test_query_enable (id UInt32) ENGINE = MergeTree ORDER BY id
  SETTINGS allow_experimental_automatic_fill_on_cluster_mode = 1, cluster_for_automatic_fill_mode = 'test_shard_localhost';

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 1 verification: CREATE TABLE contains ON CLUSTER';
SELECT count() > 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%CREATE TABLE test_query_enable%'
  AND query LIKE '%ON CLUSTER%';
DROP TABLE test_query_enable;

SELECT 'Test 2: query-level SETTINGS disable auto-fill while the profile setting is on';
-- The profile/session has the feature enabled, but the single query turns it off. With the
-- decision taken before applying query SETTINGS, the statement would still be rewritten
-- ON CLUSTER; with the fix it stays local.
SET allow_experimental_automatic_fill_on_cluster_mode = true;
SET cluster_for_automatic_fill_mode = 'test_shard_localhost';
CREATE TABLE test_query_disable (id UInt32) ENGINE = MergeTree ORDER BY id
  SETTINGS allow_experimental_automatic_fill_on_cluster_mode = 0;

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 2 verification: CREATE TABLE does NOT contain ON CLUSTER';
SELECT count() = 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%CREATE TABLE test_query_disable%'
  AND query LIKE '%ON CLUSTER%';
DROP TABLE test_query_disable;
