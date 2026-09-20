-- Foreign node types in expression positions and empty SETTINGS nodes: the SQL parser never produces them,
-- and they formatted as empty or foreign text that did not parse back. Found by json_ast_sql_parser_fuzzer
-- (strict round-trip mode on the functional-test corpus).

-- A SelectQuery, a ViewTargets and a TablesInSelectQuery as SELECT list elements; a Literal is fine.
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Identifier","name":"number"}]}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"ViewTargets"}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"TablesInSelectQuery","children":[{"type":"TablesInSelectQueryElement","table_expression":{"type":"TableExpression","database_and_table_name":{"type":"TableIdentifier","name":"t"}}}]}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]},"where":{"type":"SetQuery","changes":[{"name":"max_threads","value":{"field_type":"UInt64","value":1}}]}}]}}'); -- { serverError BAD_ARGUMENTS }

-- A SETTINGS node without any entry.
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]},"settings":{"type":"SetQuery"}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SetQuery","is_standalone":true}'); -- { serverError BAD_ARGUMENTS }

-- Parser-produced shapes still round-trip.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a, t.*, COLUMNS(''x''), (SELECT 1), {p:UInt8}, x -> x + 1, * EXCEPT b FROM t WHERE a > 1 SETTINGS max_threads = 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('SET param_p = 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM s3(''https://bucket.s3.amazonaws.com/f.csv'', SETTINGS s3_truncate_on_insert = 1)'));
