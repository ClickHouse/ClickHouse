-- Tags: no-old-analyzer
-- no-old-analyzer: `WITH RECURSIVE` needs the analyzer.

-- Inside a recursive member the name of the recursive element references the element at any
-- depth, also in a nested `SELECT` that stops inheriting with `enable_global_with_statement = 0`.
-- A view created with that setting took the name there for a table of the creating database:
-- in the stored text (so `CREATE VIEW` failed with `UNKNOWN_TABLE`, or the view read the table
-- after a reload), and in the copy of the body that stands for the reference in the in-memory
-- definition (so the creating session read the table). The live query builds 1..4.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (2);

USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'live', (WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM (SELECT id FROM src) WHERE id < 4) SELECT sum(id) FROM src)
SETTINGS enable_global_with_statement = 0;
-- The seed is resolved like any other query, so a nested `SELECT` there reads the table.
SELECT 'live seed', (WITH RECURSIVE src AS (SELECT (SELECT max(id) FROM src) AS id UNION ALL SELECT id + 1 FROM (SELECT id FROM src) WHERE id < 4) SELECT sum(id) FROM src)
SETTINGS enable_global_with_statement = 0;

SET enable_global_with_statement = 0;
CREATE VIEW v AS
    WITH RECURSIVE src AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM (SELECT id FROM src) WHERE id < 4)
    SELECT sum(id) AS s FROM src;
CREATE VIEW v_seed AS
    WITH RECURSIVE src AS (SELECT (SELECT max(id) FROM src) AS id UNION ALL SELECT id + 1 FROM (SELECT id FROM src) WHERE id < 4)
    SELECT sum(id) AS s FROM src;
SET enable_global_with_statement = 1;

SELECT 'view', s FROM v;
SELECT 'view seed', s FROM v_seed;
SELECT 'stored is bare', position(create_table_query, '.src') = 0 FROM system.tables
WHERE database = currentDatabase() AND name = 'v';

DETACH TABLE v;
ATTACH TABLE v;
DETACH TABLE v_seed;
ATTACH TABLE v_seed;
SELECT 'reload', s FROM v;
SELECT 'reload seed', s FROM v_seed;

USE {CLICKHOUSE_DATABASE:Identifier};
SELECT 'read elsewhere', s FROM {CLICKHOUSE_DATABASE_1:Identifier}.v;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
