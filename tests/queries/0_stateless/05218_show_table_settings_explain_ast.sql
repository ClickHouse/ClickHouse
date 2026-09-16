-- `EXPLAIN AST` of the statement, which prints the node's id. Nothing else reaches
-- `ASTShowTableSettingsQuery::getID`: the tree hash is compared only in debug builds
-- (`executeQuery.cpp`, `#ifndef NDEBUG`), and `clone` is called only by the AST fuzzer.
--
-- `EXPLAIN AST` only parses, so the names below are never resolved and need no table.

SELECT '-- the bare form';
EXPLAIN AST SHOW TABLE SETTINGS FROM explain_ast_no_such_table;

SELECT '-- CHANGED and a negated case-insensitive pattern parse into the same node';
EXPLAIN AST SHOW CHANGED TABLE SETTINGS FROM explain_ast_no_such_table NOT ILIKE 'a%';

SELECT '-- and a database-qualified name';
EXPLAIN AST SHOW TABLE SETTINGS FROM system.one;
