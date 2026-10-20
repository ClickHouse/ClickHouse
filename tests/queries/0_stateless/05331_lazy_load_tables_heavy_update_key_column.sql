-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.
-- no-shared-merge-tree: the table engine is replaced.

-- With `lazy_load_tables` the catalog holds a stand-in for a table until the table is first accessed.
-- A heavy `ALTER ... UPDATE` on an untouched stand-in used to take the metadata snapshot of the stand-in,
-- which has no sorting key, so an update of a key column passed the upfront check.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1, 10), (2, 20);

-- A stand-in appears when the database is loaded, so the table is a stand-in again after a re-attach.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'stand-in', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t UPDATE a = a + 10 WHERE 1 SETTINGS alter_update_mode = 'heavy'; -- { serverError CANNOT_UPDATE_COLUMN }

SELECT 'mutations', count() FROM system.mutations WHERE database = currentDatabase() AND table = 't';
SELECT 'rows', a, b FROM {CLICKHOUSE_DATABASE_1:Identifier}.t ORDER BY a;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
