-- Tags: no-replicated-database
-- no-replicated-database: hypothetical objects are session-scoped, and the test re-attaches an `Atomic` database.

-- https://github.com/ClickHouse/ClickHouse/issues/122297
-- In a database with `lazy_load_tables = 1`, a re-attached table is a `StorageTableProxy`. Hypothetical index and
-- projection DDL and `EXPLAIN WHATIF` refused it as not a `MergeTree` table, both before and after it was loaded.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t0 (c0 UInt64, c1 UInt64) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t1 (c0 UInt64, c1 UInt64) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t2 (c0 UInt64, c1 UInt64) ENGINE = MergeTree ORDER BY c0;

-- Re-attach the database so the tables become unloaded lazy proxies.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'a drop leaves the table unloaded';
DROP HYPOTHETICAL INDEX IF EXISTS x ON t0;
DROP HYPOTHETICAL PROJECTION IF EXISTS x ON t0;
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't0';

SELECT 'index on an unloaded table';
CREATE HYPOTHETICAL INDEX hi0 ON t0 (c0) TYPE minmax GRANULARITY 1;
DROP HYPOTHETICAL INDEX hi0 ON t0;

SELECT 'projection on a loaded table';
SELECT count() FROM t1 FORMAT Null;
CREATE HYPOTHETICAL PROJECTION hp0 ON t1 (SELECT c0, c1 ORDER BY c1);
DROP HYPOTHETICAL PROJECTION IF EXISTS x ON t1;
DROP HYPOTHETICAL PROJECTION hp0 ON t1;

SELECT 'EXPLAIN WHATIF on an unloaded empty table';
SELECT count() > 0 FROM (EXPLAIN WHATIF SELECT * FROM t2 WHERE c1 = 5);
CREATE HYPOTHETICAL INDEX hi1 ON t2 (c1) TYPE minmax GRANULARITY 1;
SELECT replaceRegexpAll(trim(explain), ' +', ' ') AS line
FROM (EXPLAIN WHATIF SELECT * FROM t2 WHERE c1 = 5)
WHERE match(explain, '^  parts:|^With |^\\s+status:|^\\s+reason:')
ORDER BY line;

USE {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
