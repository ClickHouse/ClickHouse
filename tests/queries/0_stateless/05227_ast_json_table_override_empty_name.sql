-- A standalone `TableOverride` node must carry a table name: `ASTTableOverride::formatImpl` builds an
-- `ASTIdentifier` from it, and that constructor asserts a non-empty name. Found by json_ast_sql_parser_fuzzer.
SELECT formatQueryFromJSON('{"type":"TableOverride"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"TableOverride","table_name":"","is_standalone":true}'); -- { serverError BAD_ARGUMENTS }

-- A non-standalone override (the `EXPLAIN TABLE OVERRIDE` shape) has no name by design and still round-trips.
SELECT formatQueryFromJSON(parseQueryToJSON('EXPLAIN TABLE OVERRIDE mysql(''host'', ''db'', ''t'', ''u'', ''p'') PARTITION BY toYYYYMM(d)'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE DATABASE db ENGINE = MaterializedMySQL(''host:3306'', ''db'', ''user'', ''pass'') TABLE OVERRIDE t (PARTITION BY x ORDER BY x)'));
