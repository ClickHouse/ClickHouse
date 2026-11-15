-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.
-- no-shared-merge-tree: the table engine is replaced, and it keeps parts elsewhere.

-- With `lazy_load_tables` the catalog holds a stand-in for a table, and the stand-in keeps wrapping the
-- table after it is loaded. `DELETE`, `UPDATE`, `ALTER ... DELETE`, `OPTIMIZE ... DRY RUN`,
-- `CREATE HYPOTHETICAL INDEX`, `CREATE TABLE ... AS` and `CLONE AS` asked the stand-in rather than the
-- table what it is, and refused a `MergeTree` table or copied only its columns.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a PARTITION BY a % 2
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t SELECT number, number FROM numbers(10);

-- A stand-in appears when the database is loaded, so every round below starts from a re-attach.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'stand-in', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';
DELETE FROM t WHERE a = 0;
SELECT 'lightweight delete', count() FROM t;

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

UPDATE t SET b = 100 WHERE a = 1 SETTINGS enable_lightweight_update = 1;
SELECT 'lightweight update', b FROM t WHERE a = 1;

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

ALTER TABLE t DELETE WHERE a = 2 SETTINGS mutations_sync = 2;
SELECT 'mutation', count() FROM t;

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

-- The parts do not exist, so the table is recognized as `MergeTree` if this is what it complains about.
OPTIMIZE TABLE t DRY RUN PARTS 'x_1_1_0', 'x_2_2_0'; -- { serverError BAD_DATA_PART_NAME, NO_SUCH_DATA_PART }

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE HYPOTHETICAL INDEX idx_b ON t (b) TYPE minmax GRANULARITY 1;
SELECT 'hypothetical index', 'ok';

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE t_as AS t ENGINE = MergeTree;
SELECT 'create as', sorting_key, partition_key FROM system.tables WHERE database = currentDatabase() AND name = 't_as';

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE t_clone CLONE AS t;
SELECT 'clone as', count() FROM t_clone;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
