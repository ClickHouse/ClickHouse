-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: doesn't support databases without UUID

-- A `SYSTEM RESTART REPLICA` that fails because the table definition cannot be analyzed
-- must leave the table in place instead of making it disappear until server restart.

-- The tables live in a Memory database, which never writes table metadata: a table whose constraint reads a missing
-- table cannot be loaded, so a leftover would stop the server starting. Cleanup is one `DROP DATABASE`, the one drop a
-- stress run never ignores; the fuzzer pin keeps this file's own DDL from being replayed under another name.
SET ast_fuzzer_any_query = 0;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Memory;
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE t_probe (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_check (x UInt64, CONSTRAINT c CHECK x < (SELECT count() + 1000 FROM t_probe))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_check', 'r1') ORDER BY tuple();

-- The constraint now references a missing table.
DROP TABLE t_probe SYNC SETTINGS check_table_dependencies = 0;

SYSTEM RESTART REPLICA t_check; -- { serverError UNKNOWN_TABLE }

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_check';
SELECT count() FROM t_check;

-- Once the referenced table is back, the restart succeeds.
CREATE TABLE t_probe (k UInt64) ENGINE = MergeTree ORDER BY k;
SYSTEM RESTART REPLICA t_check;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_check';
SELECT count() FROM t_check;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
