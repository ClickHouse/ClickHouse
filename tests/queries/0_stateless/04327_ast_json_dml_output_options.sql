-- Regression tests for two AST JSON review findings:
--
--  1. `UPDATE` / `DELETE` / `CREATE INDEX` / `DROP INDEX` are parsed by their own
--     top-level parsers, not by `ParserQueryWithOutput`, so they cannot carry an
--     `INTO OUTFILE` / `FORMAT` / `COMPRESSION` suffix. `UPDATE` / `DELETE` support
--     only a query-local `SETTINGS` clause; the index statements support none.
--     Their JSON (de)serialization must not restore the full output suffix, so
--     `clickhouse_json` cannot build an AST the SQL parser could never produce.
--
--  2. `ParserQueryWithOutput` accepts only specific node types for the output
--     options (`out_file` / `compression` are string literals, `compression_level`
--     is a numeric literal, `format_ast` is an identifier, `settings_ast` is a
--     `SetQuery`). The JSON reader must validate the concrete node type so malformed
--     `clickhouse_json` is rejected with a parse error instead of reaching a downstream
--     `as<...>` cast as a logical exception (or formatting invalid SQL).

-- (1) DML round-trips: the query-local `SETTINGS` survives for `UPDATE` / `DELETE`,
-- and the index statements round-trip unchanged:
SELECT formatQueryFromJSON(parseQueryToJSON('UPDATE t SET x = 1 WHERE 1 SETTINGS mutations_sync = 2'));
SELECT formatQueryFromJSON(parseQueryToJSON('DELETE FROM t WHERE 1 SETTINGS mutations_sync = 2'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE INDEX idx ON t (x) TYPE minmax GRANULARITY 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('DROP INDEX idx ON t'));

-- (1) An `UPDATE` JSON that injects a `format_ast` cannot produce `UPDATE ... FORMAT Null`:
-- the output options are not read for this query type, so the extra field is ignored.
SELECT formatQueryFromJSON('{"type":"UpdateQuery","table":{"type":"Identifier","name":"t"},"assignments":{"type":"ExpressionList","children":[{"type":"Assignment","column_name":"x","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}]},"predicate":{"type":"Literal","value":{"field_type":"UInt64","value":1}},"format_ast":{"type":"Identifier","name":"Null"}}');

-- (2) Output options that are normally accepted (for query types parsed via
-- `ParserQueryWithOutput`) must round-trip when well-typed:
SELECT formatQueryFromJSON(parseQueryToJSON('SHOW TABLES INTO OUTFILE \'f\' COMPRESSION \'gz\' LEVEL 3'));

-- (2) Wrong node types for the output options are rejected at the JSON boundary:
-- `out_file` must be a string literal, not an identifier:
SELECT formatQueryFromJSON('{"type":"ShowTablesQuery","out_file":{"type":"Identifier","name":"f"}}'); -- { serverError BAD_ARGUMENTS }
-- `out_file` must be a string literal, not a numeric one:
SELECT formatQueryFromJSON('{"type":"ShowTablesQuery","out_file":{"type":"Literal","value":{"field_type":"UInt64","value":1}}}'); -- { serverError BAD_ARGUMENTS }
-- `format_ast` must be an identifier, not a literal:
SELECT formatQueryFromJSON('{"type":"ShowTablesQuery","format_ast":{"type":"Literal","value":{"field_type":"String","value":"JSON"}}}'); -- { serverError BAD_ARGUMENTS }
-- `settings_ast` must be a `SetQuery`, not an identifier:
SELECT formatQueryFromJSON('{"type":"ShowTablesQuery","settings_ast":{"type":"Identifier","name":"x"}}'); -- { serverError BAD_ARGUMENTS }
-- `compression` must be a string literal, not a numeric one:
SELECT formatQueryFromJSON('{"type":"ShowTablesQuery","out_file":{"type":"Literal","value":{"field_type":"String","value":"f"}},"compression":{"type":"Literal","value":{"field_type":"UInt64","value":1}}}'); -- { serverError BAD_ARGUMENTS }
-- `compression_level` must be a numeric literal, not a string one:
SELECT formatQueryFromJSON('{"type":"ShowTablesQuery","out_file":{"type":"Literal","value":{"field_type":"String","value":"f"}},"compression":{"type":"Literal","value":{"field_type":"String","value":"gz"}},"compression_level":{"type":"Literal","value":{"field_type":"String","value":"x"}}}'); -- { serverError BAD_ARGUMENTS }
