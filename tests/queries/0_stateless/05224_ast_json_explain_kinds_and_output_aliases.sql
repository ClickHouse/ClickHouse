-- Non-TEXT EXPLAIN kinds accept only the statements ParserExplainQuery can hand them.
SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN AST","query":{"type":"Literal","value":{"field_type":"UInt64","value":1}}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN SYNTAX","query":{"type":"Literal","value":{"field_type":"UInt64","value":1}}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN","query":{"type":"SetQuery","is_standalone":true,"changes":[{"name":"max_threads","value":{"field_type":"UInt64","value":1}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN PIPELINE","query":{"type":"SetQuery","is_standalone":true,"changes":[{"name":"max_threads","value":{"field_type":"UInt64","value":1}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN QUERY TREE","query":{"type":"SetQuery","is_standalone":true,"changes":[{"name":"max_threads","value":{"field_type":"UInt64","value":1}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN AST","query":{"type":"SetQuery","is_standalone":false,"changes":[{"name":"max_threads","value":{"field_type":"UInt64","value":1}}]}}'); -- { serverError BAD_ARGUMENTS }

-- A top-level SELECT is always wrapped by the parser and the interpreters of these kinds require the
-- wrapper, so a bare SelectQuery would format to SQL that does not execute the same way as the JSON.
SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN QUERY TREE","query":{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN","query":{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN AST","query":{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN","query":{"type":"SelectIntersectExceptQuery","final_operator":"EXCEPT ALL","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}},{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN TEXT","query":{"type":"SelectIntersectExceptQuery","final_operator":"EXCEPT ALL","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}},{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}'); -- { serverError BAD_ARGUMENTS }

-- Parser-producible shapes still round-trip.
SELECT formatQueryFromJSON(parseQueryToJSON('EXPLAIN AST SET max_threads = 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('EXPLAIN AST USE default'));
SELECT formatQueryFromJSON(parseQueryToJSON('EXPLAIN SYNTAX INSERT INTO t SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('EXPLAIN PIPELINE INSERT INTO t SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('EXPLAIN SYSTEM FLUSH LOGS'));
SELECT formatQueryFromJSON(parseQueryToJSON('EXPLAIN QUERY TREE SELECT 1'));

-- Output-option nodes cannot carry aliases the parser never produces.
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]},"format_ast":{"type":"Identifier","name":"CSV","alias":"x"}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]},"out_file":{"type":"Literal","value":{"field_type":"String","value":"/dev/null"},"alias":"x"}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN TEXT","query":{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}},"format_ast":{"type":"Identifier","name":"CSV","alias":"x"}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 INTO OUTFILE ''/dev/null'' COMPRESSION ''gzip'' LEVEL 3 FORMAT CSV SETTINGS max_threads = 1'));
