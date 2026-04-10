-- Tags: zookeeper, no-parallel, no-replicated-database, no-ordinary-database
-- no-replicated-database: this test explicitly creates a Replicated database to verify auto-fill ON CLUSTER removal

-- Verify that maybeRemoveOnCluster correctly strips the auto-filled
-- ON CLUSTER clause for Replicated databases, without needing
-- ignore_on_cluster_for_replicated_database = true.
-- This is the regression test for the isAutoFillOnCluster() branch.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier}
  ENGINE = Replicated('/clickhouse/databases/03789_repl_db_{database}', 'shard1', 'replica1');

USE {CLICKHOUSE_DATABASE:Identifier};

SET distributed_ddl_output_mode='none';
SET allow_experimental_automatic_fill_on_cluster_mode = true;
SET cluster_for_automatic_fill_mode = 'test_shard_localhost';
-- Explicitly keep the default: auto-filled ON CLUSTER must be removed
-- by the isAutoFillOnCluster() path, not by this setting.
SET ignore_on_cluster_for_replicated_database = false;

SELECT 'Test 1: CREATE TABLE in Replicated DB with auto-fill succeeds';
CREATE TABLE test_repl_auto (id UInt32, value String) ENGINE = ReplicatedMergeTree ORDER BY id;

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 1 verification: CREATE TABLE does NOT contain ON CLUSTER';
SELECT count() = 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%CREATE TABLE test_repl_auto%'
  AND query LIKE '%ON CLUSTER%';

SELECT 'Test 2: ALTER TABLE in Replicated DB with auto-fill succeeds';
ALTER TABLE test_repl_auto ADD COLUMN new_col UInt64 DEFAULT 0;

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 2 verification: ALTER TABLE does NOT contain ON CLUSTER';
SELECT count() = 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%ALTER TABLE test_repl_auto%'
  AND query LIKE '%ON CLUSTER%';

SELECT 'Test 3: DROP TABLE in Replicated DB with auto-fill succeeds';
DROP TABLE test_repl_auto;

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 3 verification: DROP TABLE does NOT contain ON CLUSTER';
SELECT count() = 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%DROP TABLE test_repl_auto%'
  AND query LIKE '%ON CLUSTER%';

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
