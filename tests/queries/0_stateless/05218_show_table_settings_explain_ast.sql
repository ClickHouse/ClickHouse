-- `EXPLAIN AST` of the statement, which prints the node's id.
--
-- `EXPLAIN AST` only parses, so the names below are never resolved and need no table.

SELECT '-- the bare form';
EXPLAIN AST SHOW TABLE SETTINGS FROM explain_ast_no_such_table;

SELECT '-- CHANGED and a negated case-insensitive pattern parse into the same node';
EXPLAIN AST SHOW CHANGED TABLE SETTINGS FROM explain_ast_no_such_table NOT ILIKE 'a%';

SELECT '-- and a database-qualified name';
EXPLAIN AST SHOW TABLE SETTINGS FROM system.one;
