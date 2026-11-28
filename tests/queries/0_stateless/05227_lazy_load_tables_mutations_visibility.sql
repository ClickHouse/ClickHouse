-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.
-- no-shared-merge-tree: the table engine is replaced, and it keeps mutations elsewhere.

-- With `lazy_load_tables` the catalog holds a stand-in for a table until the table is first accessed.
-- A stand-in is not a `MergeTreeData`, and the stand-in keeps wrapping the table after it is loaded,
-- so `system.mutations` used to miss a lazily loaded table for as long as the server ran.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1), (2);
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t DELETE WHERE a = 1 SETTINGS mutations_sync = 2;

-- A stand-in appears when the database is loaded, so the table is a stand-in again after a re-attach.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'stand-in', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';
-- Reading a system table only inspects tables and must not load them.
SELECT 'in system.mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't';

SELECT 'rows', count() FROM t;

SELECT 'loaded', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';
SELECT 'in system.mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't';

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
