-- JSON AST shapes the SQL parser never produces: empty or list-valued slots that formatted as empty text
-- and did not parse back. Found by json_ast_sql_parser_fuzzer (strict round-trip mode).

-- Empty SELECT list, list in a scalar slot, list as a SELECT element.
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList"}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"}]},"where":{"type":"ExpressionList"}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Identifier","name":"a"},{"type":"ExpressionList"}]}}]}}'); -- { serverError BAD_ARGUMENTS }

-- Empty FROM list and an element without a table expression.
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Asterisk"}]},"tables":{"type":"TablesInSelectQuery"}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Asterisk"}]},"tables":{"type":"TablesInSelectQuery","children":[{"type":"TablesInSelectQueryElement"}]}}]}}'); -- { serverError BAD_ARGUMENTS }

-- Empty REPLACE (...) and a list as the KILL condition.
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Asterisk","transformers":{"type":"ColumnsTransformerList","children":[{"type":"ColumnsReplaceTransformer"}]}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"KillQueryQuery","kill_type":"Query","where_expression":{"type":"ExpressionList"}}'); -- { serverError BAD_ARGUMENTS }

-- The parser-produced shapes still round-trip.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a, * REPLACE (b + 1 AS b) FROM t WHERE c > 0 GROUP BY GROUPING SETS ((a), ()) LIMIT 1 BY a'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t ARRAY JOIN arr AS x'));
SELECT formatQueryFromJSON(parseQueryToJSON('KILL QUERY WHERE query_id = ''x'' SYNC'));
