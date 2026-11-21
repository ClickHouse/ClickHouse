-- Regression test: `RENAME TABLE` and `EXCHANGE TABLES` across databases must recompute the dependencies of the tables
-- whose definition is resolved against their own database (`Alias('t')`, an ordinary view with a source written without
-- a database) against the destination database, as a metadata reload would, instead of keeping the edges of the source one.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Atomic;

SET check_referential_table_dependencies = 1;

SELECT '-- Alias, RENAME TABLE';
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.alias_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.alias_src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.alias_src VALUES (1);
INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.alias_src VALUES (2);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.alias_own ENGINE = Alias('alias_src');

RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.alias_own TO {CLICKHOUSE_DATABASE_2:Identifier}.alias_own;

SELECT * FROM {CLICKHOUSE_DATABASE_2:Identifier}.alias_own;
DROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.alias_src; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.alias_src;

SELECT '-- Alias, EXCHANGE TABLES';
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.s1 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.s2 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.s1 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.s2 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.alias_exchanged ENGINE = Alias('s1');
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.alias_exchanged ENGINE = Alias('s2');

EXCHANGE TABLES {CLICKHOUSE_DATABASE_1:Identifier}.alias_exchanged AND {CLICKHOUSE_DATABASE_2:Identifier}.alias_exchanged;

DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.s2; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.s1; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.s1;
DROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.s2;

SELECT '-- ordinary view, RENAME TABLE';
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.view_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.view_src (id UInt64) ENGINE = MergeTree ORDER BY id;

USE {CLICKHOUSE_DATABASE_1:Identifier};
-- The temporary table shadowing the source is what keeps the stored definition of the view unqualified:
-- `AddDefaultDatabaseVisitor` does not qualify names of session-local external tables.
CREATE TEMPORARY TABLE view_src (id UInt64) ENGINE = Memory;
CREATE VIEW view_own AS SELECT * FROM view_src;
DROP TEMPORARY TABLE view_src;

-- Guard the precondition: the source of the view is stored unqualified.
SELECT 'source is stored unqualified', position(create_table_query, '.view_src') = 0
FROM system.tables WHERE database = currentDatabase() AND name = 'view_own';
USE {CLICKHOUSE_DATABASE:Identifier};

RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.view_own TO {CLICKHOUSE_DATABASE_2:Identifier}.view_own;

SELECT if(database = {CLICKHOUSE_DATABASE_1:String}, 'db1', 'db2') AS db, dependencies_table
FROM system.tables WHERE name = 'view_src' AND database IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String}) ORDER BY db;

DROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.view_src; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.view_src;

SET check_referential_table_dependencies = 0;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
