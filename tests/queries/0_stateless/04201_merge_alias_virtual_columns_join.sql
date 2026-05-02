-- Test: exercises `StorageMerge` virtual columns `_table`/`_database` qualified by table alias
-- when `merge()` is used with `AS alias` and combined with `JOIN` (forces processed_stage > FetchColumns,
-- which calls `modified_query_info.query_tree->toAST()` at StorageMerge.cpp:1296).
-- Covers: src/Storages/StorageMerge.cpp:1065 — replacement_table_expression->setAlias(...)
--         src/Interpreters/InterpreterSelectQueryAnalyzer.cpp:214 — replacement_table_expression->setAlias(...)
-- Without alias preservation, references to `m._table` / `m.x` in the rebuilt query tree would not
-- resolve after the table_expression is replaced for each child table.

DROP TABLE IF EXISTS t_61298_a;
DROP TABLE IF EXISTS t_61298_b;
DROP TABLE IF EXISTS t_61298_join;

CREATE TABLE t_61298_a (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_61298_b (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_61298_join (y UInt32, name String) ENGINE = MergeTree ORDER BY y;

INSERT INTO t_61298_a VALUES (1);
INSERT INTO t_61298_b VALUES (2);
INSERT INTO t_61298_join VALUES (1, 'one'), (2, 'two');

SELECT '--- merge() AS m: alias-qualified virtual column ---';
SELECT m._table FROM merge(currentDatabase(), 't_61298_(a|b)') AS m ORDER BY m.x;

SELECT '--- merge() AS m: alias-qualified _database ---';
SELECT (m._database = currentDatabase()) AS db_match FROM merge(currentDatabase(), 't_61298_(a|b)') AS m ORDER BY m.x;

SELECT '--- merge() AS m: WHERE filter on alias-qualified _table ---';
SELECT m.x, m._table FROM merge(currentDatabase(), 't_61298_(a|b)') AS m WHERE m._table = 't_61298_a' ORDER BY m.x;

SELECT '--- merge() AS m JOIN ... : forces toAST() conversion path ---';
SELECT m._table, j.name FROM merge(currentDatabase(), 't_61298_(a|b)') AS m
INNER JOIN t_61298_join AS j ON m.x = j.y
ORDER BY m._table;

SELECT '--- merge() AS m: GROUP BY alias-qualified _table ---';
SELECT m._table, count() FROM merge(currentDatabase(), 't_61298_(a|b)') AS m GROUP BY m._table ORDER BY m._table;

DROP TABLE t_61298_a;
DROP TABLE t_61298_b;
DROP TABLE t_61298_join;
