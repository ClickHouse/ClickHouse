-- Regression test: an `Alias` table whose target is written without a database resolves the target against
-- the database of the alias itself. After `RENAME DATABASE` the alias must follow into the new database, both
-- in memory (reads and writes through it) and in the referential dependency graph, as a reload would.
-- An `Alias` with a qualified target keeps naming the database written in its definition.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (1);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.alias_own ENGINE = Alias('src');

-- A table with the same name in the current database must not be picked up by the alias.
DROP TABLE IF EXISTS src;
CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (100);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.alias_qualified ENGINE = Alias(currentDatabase(), 'src');

SELECT 'before rename', * FROM {CLICKHOUSE_DATABASE_1:Identifier}.alias_own;

RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier};

SELECT 'after rename', * FROM {CLICKHOUSE_DATABASE_2:Identifier}.alias_own;
INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.alias_own VALUES (2);
SELECT 'source after insert', groupArray(id) FROM (SELECT id FROM {CLICKHOUSE_DATABASE_2:Identifier}.src ORDER BY id);
SELECT 'qualified alias', * FROM {CLICKHOUSE_DATABASE_2:Identifier}.alias_qualified;

-- The referential dependency of the alias follows the source into the new database.
SET check_referential_table_dependencies = 1;
DROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.src; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE src; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.alias_own;
DROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.src;
DROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.alias_qualified;
DROP TABLE src;

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
