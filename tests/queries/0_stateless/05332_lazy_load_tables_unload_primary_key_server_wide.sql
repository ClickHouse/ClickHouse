-- Tags: no-parallel, no-replicated-database, no-shared-merge-tree
-- no-parallel: the server-wide `SYSTEM UNLOAD PRIMARY KEY` unloads the primary keys of the tables of the concurrent tests.
-- no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.
-- no-shared-merge-tree: the table engine is replaced.

-- With `lazy_load_tables` the catalog holds a stand-in for a table until the table is first accessed.
-- The server-wide `SYSTEM UNLOAD PRIMARY KEY` only inspects tables and must not load them.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1), (2);

-- A stand-in appears when the database is loaded.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'stand-in', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';
SYSTEM UNLOAD PRIMARY KEY;
SELECT 'still stand-in', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
