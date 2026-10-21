-- The table function `eval` resolves to a StorageView. `EXPLAIN AST optimize = 1` analyzes the query,
-- so `*` is expanded into the generated column while the table function itself stays in the query,
-- the same way `view` is handled.

SET allow_experimental_eval_table_function = 1;

SELECT
    countIf(explain ILIKE '%Function eval%') AS table_function_kept,
    countIf(explain ILIKE '%Asterisk%') AS asterisk_left
FROM (EXPLAIN AST optimize = 1 SELECT * FROM eval('SELECT 5 AS x'));

-- `view` is the sibling that must stay handled the same way.
SELECT
    countIf(explain ILIKE '%Function view%') AS table_function_kept,
    countIf(explain ILIKE '%Asterisk%') AS asterisk_left
FROM (EXPLAIN AST optimize = 1 SELECT * FROM view(SELECT 5 AS x));

-- Every arm of a UNION is analyzed.
SELECT
    countIf(explain ILIKE '%Function eval%') AS table_function_kept,
    countIf(explain ILIKE '%Asterisk%') AS asterisk_left
FROM (EXPLAIN AST optimize = 1 SELECT * FROM eval('SELECT 5 AS x') UNION ALL SELECT 6 AS x);

-- The parser lowercases a table function name, so an upper-case spelling takes the same path.
SELECT
    countIf(explain ILIKE '%Function eval%') AS table_function_kept,
    countIf(explain ILIKE '%Asterisk%') AS asterisk_left
FROM (EXPLAIN AST optimize = 1 SELECT * FROM EVAL('SELECT 5 AS x'));

-- A SELECT inside a non-SELECT query is analyzed by EXPLAIN SYNTAX when the query tree passes are
-- requested. INSERT is not a valid subquery, so the formatted output is compared directly.
CREATE TABLE t_05233 (x UInt8) ENGINE = Memory;
EXPLAIN SYNTAX run_query_tree_passes = 1 INSERT INTO t_05233 SELECT * FROM eval('SELECT 5 AS x');
DROP TABLE t_05233;
