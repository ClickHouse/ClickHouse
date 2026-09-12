-- A common table expression is expanded before the database is filled into a mutation command, so a
-- table identifier hidden by an expression alias of the same name is left unqualified. With
-- `enable_global_with_statement` disabled the alias is not visible in the subquery, so the identifier
-- names a table there and has to be resolved in the database of the updated table, not of the session.
-- The source table exists in both databases with a different row, so an expression resolved in the
-- database of the session silently reads the wrong row instead of failing.
-- The reference is one `SELECT` deeper than the alias: only a lookup in an enclosing scope is
-- disabled, so a reference in the select that declares the alias reads it either way.
-- The old analyzer resolves a common table expression in a subquery of a mutation as a table, so the
-- analyzer is requested explicitly, as in `04693_merge_table_function_in_mutation`.

CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (99);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (2);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.u (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.u VALUES (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0), (99, 0);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1, 0), (2, 0), (3, 0), (99, 0);

-- Every row starts at 0 and every expected value is non-zero, so a statement that does not update the
-- row at all also fails. The row `id = 99` exists so that a predicate resolved in the database of the
-- session updates the wrong row rather than no row.

-- An assignment reads the table of the updated database (2), not the table of the session (99).
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src SETTINGS enable_global_with_statement = 0))
    WHERE id = 1 SETTINGS enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 1;

-- The `ALTER TABLE ... UPDATE` spelling of the same expression answers the same.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src SETTINGS enable_global_with_statement = 0))
    WHERE id = 1 SETTINGS mutations_sync = 2, enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

-- The predicate is expanded by a separate call, so it is asserted separately: it marks the row the
-- updated database names (2), not the one the session database names (99).
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = 11
    WHERE id IN (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src SETTINGS enable_global_with_statement = 0))
    SETTINGS enable_analyzer = 1;
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE v = 11;

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = 11
    WHERE id IN (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src SETTINGS enable_global_with_statement = 0))
    SETTINGS mutations_sync = 2, enable_analyzer = 1;
SELECT id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE v = 11;

-- The effective value of the setting is read, so a profile that carries it answers the same as the
-- setting written out.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src SETTINGS compatibility = '20.3'))
    WHERE id = 3 SETTINGS enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 3;

-- With the setting at its default the alias IS visible in the subquery, so the same reference reads
-- the common table expression (7) and must not be qualified as a table.
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src))
    WHERE id = 4 SETTINGS enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 4;

-- A reference in the select that declares the alias keeps reading the alias, and the table its body
-- names is resolved in the updated database (2).
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH c AS (SELECT max(id) AS m FROM src) SELECT m FROM c)
    WHERE id = 5 SETTINGS enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 5;

-- An identifier that no alias hides was always resolved in the updated database (2).
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (SELECT max(id) FROM src)
    WHERE id = 6 SETTINGS enable_analyzer = 1;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 6;

DROP TABLE src;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
