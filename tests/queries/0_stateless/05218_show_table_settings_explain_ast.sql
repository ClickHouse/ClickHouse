-- `EXPLAIN AST` of the statement, which prints the node's id. Nothing else reaches
-- `ASTShowTableSettingsQuery::getID`: the tree hash is compared only in debug builds
-- (`executeQuery.cpp`, `#ifndef NDEBUG`), and `clone` is called only by the AST fuzzer.

DROP TABLE IF EXISTS explain_ast_mt;
CREATE TABLE explain_ast_mt (a UInt64) ENGINE = MergeTree ORDER BY a;

SELECT '-- the bare form';
EXPLAIN AST SHOW TABLE SETTINGS FROM explain_ast_mt;

SELECT '-- CHANGED and a negated case-insensitive pattern parse into the same node';
EXPLAIN AST SHOW CHANGED TABLE SETTINGS FROM explain_ast_mt NOT ILIKE 'a%';

SELECT '-- and a database-qualified name';
EXPLAIN AST SHOW TABLE SETTINGS FROM system.one;

DROP TABLE explain_ast_mt;
