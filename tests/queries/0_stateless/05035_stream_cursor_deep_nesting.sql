-- The `CURSOR` clause of `STREAM` is parsed by a helper that recurses directly instead of going
-- through `IParserBase::parse`, so `max_parser_depth` was not in effect and a deeply nested cursor
-- exhausted the thread stack.

SELECT parseQueryToJSON(concat('SELECT * FROM t STREAM CURSOR ', repeat('{''a'': ', 100000), '10', repeat('}', 100000)))
SETTINGS max_query_size = 100000000; -- { serverError TOO_DEEP_RECURSION }

SELECT parseQueryToJSON('SELECT * FROM t STREAM CURSOR {''a'': {''b'': 10}}') IS NOT NULL;

-- A cursor key written with dots describes the same tree as the nested form above, so it is bounded
-- too, however the query reaches the server: as SQL text, or as an AST in JSON.

SELECT parseQueryToJSON(concat('SELECT * FROM t STREAM CURSOR {''', repeat('a.', 100000), 'z'': 10}')); -- { serverError TOO_DEEP_RECURSION }

SELECT formatQuery(concat('SELECT * FROM t STREAM CURSOR {''', repeat('a.', 100000), 'z'': 10}')); -- { serverError TOO_DEEP_RECURSION }

SELECT formatQueryFromJSON(concat(
    '{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Asterisk"}]},"tables":{"type":"TablesInSelectQuery","children":[{"type":"TablesInSelectQueryElement","table_expression":{"type":"TableExpression","database_and_table_name":{"type":"TableIdentifier","name":"t"},"stream_settings":{"type":"StreamSettings","cursor_tree":{"field_type":"Map","value":[{"field_type":"Tuple","value":[{"field_type":"String","value":"',
    repeat('a.', 100000), 'z',
    '"},{"field_type":"Int64","value":10}]}]}}}}]}}]}}')); -- { serverError TOO_DEEP_RECURSION }

-- A dotted key within the limit is accepted, and is the nested cursor it describes.
SELECT formatQuery('SELECT * FROM t STREAM CURSOR {''a.b.c'': 10}');
