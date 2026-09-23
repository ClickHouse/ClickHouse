-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: that run strips the zookeeper_path argument, which is where the macro under test lives.

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- `{default_path_test}` holds `{database}`, so the stored path re-expands the database name at every load.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound (x UInt64)
    ENGINE = ReplicatedMergeTree('{default_path_test}05231_bound', 'r1') ORDER BY x;
-- `{default_name_test}` holds `{table}`, which a database rename keeps.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.table_macro (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05231/{default_name_test}', 'r1') ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.bound VALUES (1);

SELECT create_table_query LIKE '%{default_path_test}%' FROM system.tables
    WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'bound';
SELECT position(zookeeper_path, {CLICKHOUSE_DATABASE_1:String}) > 0 FROM system.replicas
    WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 'bound';

RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError NOT_IMPLEMENTED }

-- Nothing was moved, and the table still loads on its path.
SELECT name = {CLICKHOUSE_DATABASE_1:String} FROM system.databases
    WHERE name IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String});
DETACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound;
-- A detached table is answered from its stored definition: attaching under the new name would rebind it.
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError NOT_IMPLEMENTED }
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.bound VALUES (2);
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.bound;
SELECT is_readonly FROM system.replicas WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 'bound';

-- Without the bound table the rename goes through, and the `{table}`-only path moves along.
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.bound SYNC;
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier};
DETACH TABLE {CLICKHOUSE_DATABASE_2:Identifier}.table_macro;
ATTACH TABLE {CLICKHOUSE_DATABASE_2:Identifier}.table_macro;
INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.table_macro VALUES (1);
SELECT is_readonly FROM system.replicas WHERE database = {CLICKHOUSE_DATABASE_2:String} AND table = 'table_macro';
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier} SYNC;

-- A lazily loaded table is a `StorageTableProxy` until it is read; the check has to reach the table behind it.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.lazy_bound (x UInt64)
    ENGINE = ReplicatedMergeTree('{default_path_test}05231_lazy', 'r1') ORDER BY x;
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT engine FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'lazy_bound';
-- A proxy nothing has touched is answered from its stored definition, for a database rename and a table rename alike.
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError NOT_IMPLEMENTED }
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.lazy_bound TO {CLICKHOUSE_DATABASE_1:Identifier}.lazy_moved; -- { serverError NOT_IMPLEMENTED }
SELECT engine FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'lazy_bound';
-- Loaded, the table answers for itself, for both renames.
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.lazy_bound;
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError NOT_IMPLEMENTED }
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.lazy_bound TO {CLICKHOUSE_DATABASE_1:Identifier}.lazy_moved; -- { serverError NOT_IMPLEMENTED }
SELECT engine FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} AND name = 'lazy_bound';
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;

-- The renames left nothing behind under either name.
SELECT count() FROM system.tables
    WHERE database IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String}, currentDatabase());
