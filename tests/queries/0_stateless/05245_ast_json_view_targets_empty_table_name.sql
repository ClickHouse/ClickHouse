-- A `TO` target of a view with an empty `table_name` must be rejected as malformed input instead of surfacing
-- the `UNKNOWN_TABLE` error of the `StorageID` constructor. Found by json_ast_sql_parser_fuzzer.
SELECT formatQueryFromJSON('{"type":"CreateQuery","table":"mv","is_materialized_view":true,"targets":{"type":"ViewTargets","targets":[{"kind":"To","table_database":"db","table_name":""}]},"select":{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}}'); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW mv TO db.dst AS SELECT 1'));
