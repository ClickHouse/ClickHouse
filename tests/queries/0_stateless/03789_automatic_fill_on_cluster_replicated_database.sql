-- Tags: zookeeper, no-parallel, no-replicated-database, no-ordinary-database
-- no-replicated-database: this test explicitly creates a Replicated database to verify auto-fill behavior.

-- Verify that auto-fill does not add ON CLUSTER for Replicated databases,
-- since such databases coordinate DDL replication themselves and adding
-- ON CLUSTER would be redundant and would conflict with their machinery.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier}
  ENGINE = Replicated('/clickhouse/databases/03789_repl_db_{database}', 'shard1', 'replica1');

USE {CLICKHOUSE_DATABASE:Identifier};

SET distributed_ddl_output_mode='none';
SET allow_experimental_automatic_fill_on_cluster_mode = true;
SET cluster_for_automatic_fill_mode = 'test_shard_localhost';
-- Explicitly keep the default: auto-fill must skip Replicated databases on its own,
-- without relying on this setting.
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

SELECT 'Test 3: RENAME TABLE in Replicated DB with auto-fill succeeds';
RENAME TABLE test_repl_auto TO test_repl_auto_renamed;

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 3 verification: RENAME TABLE does NOT contain ON CLUSTER';
SELECT count() = 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%RENAME TABLE test_repl_auto%'
  AND query LIKE '%ON CLUSTER%';

SELECT 'Test 4: DROP TABLE in Replicated DB with auto-fill succeeds';
DROP TABLE test_repl_auto_renamed;

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 4 verification: DROP TABLE does NOT contain ON CLUSTER';
SELECT count() = 0 FROM system.query_log WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%DROP TABLE test_repl_auto%'
  AND query LIKE '%ON CLUSTER%';

SELECT 'Test 5: multi-table DROP listing a Replicated-DB table is not rewritten ON CLUSTER';
-- A multi-table `DROP TABLE a.t1, b.t2` keeps its targets in `database_and_tables` while the
-- query-level database stays empty. Operate from an ordinary current database so the guard must
-- inspect the per-target databases instead of falling back to the current one. The drop must skip
-- auto-fill because one of the listed tables lives in a Replicated database.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.test_repl_multi (id UInt32) ENGINE = ReplicatedMergeTree ORDER BY id;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.test_ord_multi (id UInt32) ENGINE = MergeTree ORDER BY id;

USE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.test_repl_multi, {CLICKHOUSE_DATABASE_1:Identifier}.test_ord_multi;

USE {CLICKHOUSE_DATABASE:Identifier};
SYSTEM FLUSH LOGS query_log;
SELECT 'Test 5 verification: multi-table DROP does NOT contain ON CLUSTER';
SELECT count() = 0 FROM system.query_log WHERE type = 'QueryFinish'
  AND query LIKE '%DROP TABLE%test_repl_multi%'
  AND query LIKE '%ON CLUSTER%';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
