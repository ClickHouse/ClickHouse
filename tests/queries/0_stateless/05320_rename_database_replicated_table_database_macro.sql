-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: that run strips the zookeeper_path argument, which is where the macro under test lives.

-- `{default_path_test}` is a configured macro whose value contains `{database}`. The stored path of such a table gets the
-- database name substituted on every load, so the table cannot follow a rename of its database. `{default_name_test}`
-- contains `{table}`, which a database rename does not change.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound (x UInt64)
    ENGINE = ReplicatedMergeTree('{default_path_test}05320_bound', 'r1') ORDER BY x;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.table_macro (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05320/{default_name_test}', 'r1') ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.bound VALUES (1);
SELECT position(zookeeper_path, {CLICKHOUSE_DATABASE_1:String}) > 0 FROM system.replicas
    WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 'bound';

-- Refused, and nothing has been moved.
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError NOT_IMPLEMENTED }
SELECT name = {CLICKHOUSE_DATABASE_1:String} FROM system.databases
    WHERE name IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String});
EXISTS TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound;

-- The table still works on its original path.
DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.bound VALUES (2);
SELECT count(), any(is_readonly) FROM {CLICKHOUSE_DATABASE_1:Identifier}.bound, system.replicas
    WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 'bound';

-- Without the bound table the rename works, and the table with `{table}` in its path is fine after it.
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound SYNC;
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier};
DETACH TABLE {CLICKHOUSE_DATABASE_2:Identifier}.table_macro;
ATTACH TABLE {CLICKHOUSE_DATABASE_2:Identifier}.table_macro;
INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.table_macro VALUES (1);
SELECT count(), any(is_readonly) FROM {CLICKHOUSE_DATABASE_2:Identifier}.table_macro, system.replicas
    WHERE database = {CLICKHOUSE_DATABASE_2:String} AND table = 'table_macro';
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier} SYNC;

-- With `lazy_load_tables` the table is a stand-in until first use. Once loaded, the stand-in must answer for it.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.lazy_bound (x UInt64)
    ENGINE = ReplicatedMergeTree('{default_path_test}05320_lazy', 'r1') ORDER BY x;
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 'lazy_bound';
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.lazy_bound;
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError NOT_IMPLEMENTED }
EXISTS TABLE {CLICKHOUSE_DATABASE_1:Identifier}.lazy_bound;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
