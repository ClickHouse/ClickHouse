-- A `SETTINGS` clause after the trailing `FORMAT` belongs to `EXPLAIN TEXT`, also when a nested `EXPLAIN`
-- over `INSERT ... SELECT` has already taken that `FORMAT` onto its own node.
EXPLAIN TEXT EXPLAIN AST INSERT INTO t SELECT 1 FORMAT JSONEachRow SETTINGS max_threads = 2;
EXPLAIN TEXT EXPLAIN SYNTAX INSERT INTO t SELECT 1 FORMAT JSONEachRow SETTINGS max_threads = 2;
EXPLAIN TEXT EXPLAIN AST INSERT INTO t SELECT 1 FORMAT JSONEachRow;
EXPLAIN TEXT INSERT INTO t SELECT 1 FORMAT JSONEachRow SETTINGS max_threads = 2;
EXPLAIN TEXT EXPLAIN AST SELECT 1 FORMAT JSONEachRow SETTINGS max_threads = 2;

-- A `SETTINGS` clause directly after the statement still belongs to the statement.
EXPLAIN TEXT SHOW TABLES SETTINGS max_threads = 2 FORMAT JSONEachRow;
EXPLAIN TEXT SHOW TABLES FORMAT JSONEachRow SETTINGS max_threads = 2;
