-- The table function `eval` resolves to a StorageView, so the AST-based arms of EXPLAIN AST and
-- EXPLAIN SYNTAX rewrite it into a subquery, the same way `view` is rewritten.

SET allow_experimental_eval_table_function = 1;

-- The rewrite replaces the `eval(...)` node with the generated query, which is what lets the
-- analysis behind it expand `*` into the generated column.
SELECT
    countIf(explain ILIKE '%Function eval%') AS table_function_node_left,
    countIf(explain ILIKE '%Identifier x%') AS asterisk_resolved
FROM (EXPLAIN AST optimize = 1 SELECT * FROM eval('SELECT 5 AS x'));

-- `view` is the sibling that was already recognised here, and must stay rewritten the same way.
SELECT
    countIf(explain ILIKE '%Function view%') AS table_function_node_left,
    countIf(explain ILIKE '%Identifier x%') AS asterisk_resolved
FROM (EXPLAIN AST optimize = 1 SELECT * FROM view(SELECT 5 AS x));

-- A UNION arm is reached through non-SELECT nodes, so each arm is rewritten on its own.
SELECT
    countIf(explain ILIKE '%Function eval%') AS table_function_node_left,
    countIf(explain ILIKE '%Identifier x%') AS asterisk_resolved
FROM (EXPLAIN AST optimize = 1 SELECT * FROM eval('SELECT 5 AS x') UNION ALL SELECT 6 AS x);

-- The parser lowercases a table function name, so an upper-case spelling takes the same path.
SELECT
    countIf(explain ILIKE '%Function eval%') AS table_function_node_left,
    countIf(explain ILIKE '%Identifier x%') AS asterisk_resolved
FROM (EXPLAIN AST optimize = 1 SELECT * FROM EVAL('SELECT 5 AS x'));

-- A non-SELECT top level reaches the same rewrite through EXPLAIN SYNTAX. INSERT is not a valid
-- subquery, so the formatted output is compared directly; `x` in place of `*` is the rewrite.
CREATE TABLE t_05233 (x UInt8) ENGINE = Memory;
EXPLAIN SYNTAX INSERT INTO t_05233 SELECT * FROM eval('SELECT 5 AS x');
DROP TABLE t_05233;
