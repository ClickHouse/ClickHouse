-- `SQL UDF` expansion drops the function's body into the query after the main qualification pass,
-- so the table names it brings in are qualified by the narrow pass instead. That pass has to honour
-- the same `WITH` scoping as the main one: inside a body that turns `enable_global_with_statement`
-- off, a common table expression of the enclosing `SELECT` is not visible, so the name is an
-- ordinary table of the database owning the definition and must be qualified. Leaving it bare made
-- the stored view read the table of whichever database queried it.

DROP DATABASE IF EXISTS db_05233;
CREATE DATABASE db_05233;

CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (11);
CREATE TABLE db_05233.src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO db_05233.src VALUES (22);

DROP FUNCTION IF EXISTS f_05233_out_of_scope;
CREATE FUNCTION f_05233_out_of_scope AS () -> (SELECT max(id) FROM src SETTINGS enable_global_with_statement = 0);
DROP FUNCTION IF EXISTS f_05233_in_scope;
CREATE FUNCTION f_05233_in_scope AS () -> (SELECT max(id) FROM src);

-- The live answers, which the stored ones have to match: the body that disables inheritance reads
-- the table, the one that inherits reads the common table expression.
SELECT 'live out of scope', (WITH src AS (SELECT 7 AS id) SELECT f_05233_out_of_scope());
SELECT 'live in scope', (WITH src AS (SELECT 7 AS id) SELECT f_05233_in_scope());

CREATE VIEW v_out_of_scope AS WITH src AS (SELECT 7 AS id) SELECT f_05233_out_of_scope() AS x;
CREATE VIEW v_in_scope AS WITH src AS (SELECT 7 AS id) SELECT f_05233_in_scope() AS x;

-- The name the enclosing `WITH` does not reach is qualified; the one it reaches is left alone.
SELECT 'stored out of scope qualified', position(create_table_query, currentDatabase() || '.src') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_out_of_scope';
SELECT 'stored in scope not qualified', position(create_table_query, currentDatabase() || '.src') = 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_in_scope';

-- Read from another database that holds a table of the same name: the answers must not move.
USE db_05233;
SELECT 'read out of scope', x FROM {CLICKHOUSE_DATABASE:Identifier}.v_out_of_scope;
SELECT 'read in scope', x FROM {CLICKHOUSE_DATABASE:Identifier}.v_in_scope;
USE {CLICKHOUSE_DATABASE:Identifier};

-- Inside a `WITH` element's own body the name is not the element: a plain element, and the seed
-- of a recursive one, read the table, so those names have to be qualified as well - the same rule
-- the main pass applies through `BodyWalk`. The recursive members after the seed do reference the
-- element and must stay bare, and a same-named element of an enclosing `SELECT` still hides the
-- table, so only the innermost declaration is masked.
DROP FUNCTION IF EXISTS f_05233_own_body;
CREATE FUNCTION f_05233_own_body AS () -> (WITH src AS (SELECT max(id) AS m FROM src) SELECT m FROM src);
DROP FUNCTION IF EXISTS f_05233_recursive_seed;
CREATE FUNCTION f_05233_recursive_seed AS () ->
    (WITH RECURSIVE src AS (SELECT max(id) AS m FROM src UNION ALL SELECT m FROM src WHERE 0) SELECT max(m) FROM src);
DROP FUNCTION IF EXISTS f_05233_nested_same_name;
CREATE FUNCTION f_05233_nested_same_name AS () ->
    (WITH src AS (SELECT 5 AS m) SELECT (WITH src AS (SELECT max(m) * 2 AS m FROM src) SELECT m FROM src));

SELECT 'live own body', f_05233_own_body();
SELECT 'live recursive seed', f_05233_recursive_seed();
SELECT 'live nested same name', f_05233_nested_same_name();

CREATE VIEW v_own_body AS SELECT f_05233_own_body() AS x;
CREATE VIEW v_recursive_seed AS SELECT f_05233_recursive_seed() AS x;
CREATE VIEW v_nested_same_name AS SELECT f_05233_nested_same_name() AS x;

SELECT 'stored own body qualified', position(create_table_query, currentDatabase() || '.src') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_own_body';
SELECT 'stored recursive seed qualified', position(create_table_query, currentDatabase() || '.src') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_recursive_seed';
-- No table is involved in the nested shape: both names are common table expressions there.
SELECT 'stored nested same name not qualified', position(create_table_query, currentDatabase() || '.src') = 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v_nested_same_name';

USE db_05233;
SELECT 'read own body', x FROM {CLICKHOUSE_DATABASE:Identifier}.v_own_body;
SELECT 'read recursive seed', x FROM {CLICKHOUSE_DATABASE:Identifier}.v_recursive_seed;
SELECT 'read nested same name', x FROM {CLICKHOUSE_DATABASE:Identifier}.v_nested_same_name;
USE {CLICKHOUSE_DATABASE:Identifier};

-- The stored text is what a reload re-derives the answer from.
DETACH TABLE v_out_of_scope;
ATTACH TABLE v_out_of_scope;
SELECT 'reload out of scope', x FROM v_out_of_scope;

DROP FUNCTION f_05233_out_of_scope;
DROP FUNCTION f_05233_in_scope;
DROP FUNCTION f_05233_own_body;
DROP FUNCTION f_05233_recursive_seed;
DROP FUNCTION f_05233_nested_same_name;
DROP DATABASE db_05233;
