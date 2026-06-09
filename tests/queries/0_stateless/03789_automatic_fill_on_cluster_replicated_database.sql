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

SELECT 'Test 5: multi-table DROP mixing a Replicated-DB table with an ordinary table is rejected';
-- A multi-table `DROP TABLE a.t1, b.t2` keeps its targets in `database_and_tables`. A single DROP
-- carries one cluster value for the whole statement, but it is later split into independent
-- single-table drops. When it mixes a target that must stay local (here a Replicated-database table)
-- with an ordinary target that should be distributed, no single value is correct: filling the cluster
-- would push the Replicated-database table through distributed DDL, while leaving it empty would run
-- the ordinary table only on the initiator. Auto-fill rejects such statements so the user issues
-- separate drops. Operate from an ordinary current database so the guard must inspect the per-target
-- databases instead of falling back to the current one.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.test_repl_multi (id UInt32) ENGINE = ReplicatedMergeTree ORDER BY id;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.test_ord_multi (id UInt32) ENGINE = MergeTree ORDER BY id;

USE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.test_repl_multi, {CLICKHOUSE_DATABASE_1:Identifier}.test_ord_multi; -- { serverError BAD_ARGUMENTS }

-- The statement was rejected, so both tables still exist; drop them with separate single-table
-- statements (each is auto-filled correctly: the Replicated-database table stays local, the
-- ordinary table is distributed ON CLUSTER).
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.test_repl_multi;
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.test_ord_multi;

USE {CLICKHOUSE_DATABASE:Identifier};

SELECT 'Test 6: DROP DATABASE of a Replicated database IS rewritten ON CLUSTER';
-- A database-level DROP is not a table DDL that a Replicated database coordinates on its own:
-- DatabaseReplicated::shouldReplicateQuery returns false for DROP DATABASE, so dropping a Replicated
-- database without ON CLUSTER would only remove the local replica. Auto-fill must add ON CLUSTER here
-- so the database is dropped on every node of the configured cluster.
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
  ENGINE = Replicated('/clickhouse/databases/03789_repl_db6_{database}', 'shard1', 'replica1');
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SYSTEM FLUSH LOGS query_log;
SELECT 'Test 6 verification: DROP DATABASE contains ON CLUSTER';
-- Match the DROP DATABASE of the Replicated database by its name. The verification SELECT itself
-- contains the literals `DROP DATABASE`, the database name and `ON CLUSTER`, but also references
-- `system.query_log`, so exclude any logged query mentioning `query_log` to avoid a self-match.
SELECT count() > 0 FROM system.query_log WHERE type = 'QueryFinish'
  AND query LIKE concat('%DROP DATABASE%', {CLICKHOUSE_DATABASE_1:String}, '%ON CLUSTER%')
  AND query NOT LIKE '%query_log%';

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
