-- ASTShowTablesQuery::updateTreeHashImpl folds `full`, `not_like` and `case_insensitive_like` into the
-- AST tree hash so the rewrite-rule matcher does not treat `SHOW FULL TABLES` and `SHOW TABLES` (or the
-- `NOT LIKE` / `ILIKE` variants) as identical. That only works if these fields survive a format -> parse
-- round-trip; otherwise the debug-build consistency check in executeQuery (parse -> format -> parse must
-- preserve the tree hash) aborts the server with an "Inconsistent AST formatting" logical error. The
-- formatter used to silently drop the `FULL` modifier and the empty-pattern `LIKE` / `ILIKE` clauses.

-- Build-independent checks that the formatter now emits these fields (escaping-agnostic booleans).
SELECT startsWith(formatQuery('SHOW FULL TABLES FROM db LIKE ''a'''), 'SHOW FULL TABLES');
SELECT startsWith(formatQuery('SHOW FULL DATABASES'), 'SHOW FULL DATABASES');
SELECT endsWith(formatQuery('SHOW TABLES NOT LIKE '''''), 'NOT LIKE ''''');
SELECT endsWith(formatQuery('SHOW TABLES ILIKE '''''), 'ILIKE ''''');

-- Executing the queries additionally exercises the debug-build parse -> format -> parse tree-hash check.
DROP TABLE IF EXISTS t_04527;
CREATE TABLE t_04527 (x UInt8) ENGINE = Memory;

-- `full` changes the result columns (name, engine), so it is semantically meaningful and must round-trip.
SHOW FULL TABLES LIKE 't_04527';
SHOW FULL DATABASES LIKE 'no_such_db_04527';
SHOW TABLES ILIKE '';
SHOW TABLES NOT LIKE '';

DROP TABLE t_04527;
