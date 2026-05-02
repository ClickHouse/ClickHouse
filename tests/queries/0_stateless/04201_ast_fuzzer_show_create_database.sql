-- Tags: no-fasttest

-- Test: exercises the clone() overrides on `ASTShowCreateDatabaseQuery` and `ASTExistsDatabaseQuery`
-- by enabling the AST fuzzer, which calls clone() on the AST before mutating and re-formatting.
-- Without the overrides (added by PR #61269), clone() would slice the AST to the parent template
-- `ASTQueryWithTableAndOutputImpl<...>` whose formatQueryImpl chassert's on `table != nullptr`
-- (these queries only set `database`, not `table`) — the original fuzzer crash described in the PR.
-- Covers: src/Parsers/TablePropertiesQueriesASTs.h — ASTShowCreateDatabaseQuery::clone, ASTExistsDatabaseQuery::clone

SET ast_fuzzer_runs = 5;
SET send_logs_level = 'fatal';

SHOW CREATE DATABASE default FORMAT TabSeparatedRaw;
EXISTS DATABASE default;
