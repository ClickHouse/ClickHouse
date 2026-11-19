-- `SETTINGS name = DEFAULT` in a subquery of a mutation restores the default of a setting the outer
-- query changed, so it decides whether a common table expression is visible there, as it does for a
-- plain `SELECT`. With `enable_global_with_statement` reset to its default (1) the reference reads the
-- common table expression (7); if the reset is ignored, the identifier names the table of the updated
-- database (2).

CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (2);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.u (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.u VALUES (1, 0), (2, 0);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1, 0);

-- The same expression as a plain `SELECT`.
SELECT (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src SETTINGS enable_global_with_statement = DEFAULT))
    SETTINGS enable_analyzer = 1, enable_global_with_statement = 0;

UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src SETTINGS enable_global_with_statement = DEFAULT))
    WHERE id = 1 SETTINGS enable_analyzer = 1, enable_global_with_statement = 0;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 1;

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t
    UPDATE v = (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src SETTINGS enable_global_with_statement = DEFAULT))
    WHERE id = 1 SETTINGS mutations_sync = 2, enable_analyzer = 1, enable_global_with_statement = 0;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 1;

-- Without the reset the setting stays disabled and the identifier names the table (2).
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u
    SET v = (WITH src AS (SELECT 7 AS id) SELECT (SELECT max(id) FROM src))
    WHERE id = 2 SETTINGS enable_analyzer = 1, enable_global_with_statement = 0;
SELECT v FROM {CLICKHOUSE_DATABASE_1:Identifier}.u WHERE id = 2;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
