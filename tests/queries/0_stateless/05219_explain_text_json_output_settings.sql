SELECT formatQueryFromJSON(concat(
    '{"type":"ExplainQuery","kind":"EXPLAIN TEXT","query":', parseQueryToJSON('SELECT 1'),
    ',"settings_ast":{"type":"SetQuery","is_standalone":true,"changes":[{"name":"max_threads","value":{"field_type":"UInt64","value":1}}]}}')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(concat(
    '{"type":"ExplainQuery","kind":"EXPLAIN TEXT","query":', parseQueryToJSON('SELECT 1'),
    ',"settings_ast":{"type":"SetQuery"}}')); -- { serverError BAD_ARGUMENTS }

-- The shared output-settings reader also protects ordinary queries.
SELECT formatQueryFromJSON(concat(
    '{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[', parseQueryToJSON('SELECT 1'),
    ']},"settings_ast":{"type":"SetQuery","is_standalone":true,"changes":[{"name":"max_threads","value":{"field_type":"UInt64","value":1}}]}}')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(concat(
    '{"type":"SelectWithUnionQuery","list_of_selects":{"type":"ExpressionList","children":[', parseQueryToJSON('SELECT 1'),
    ']},"settings_ast":{"type":"SetQuery"}}')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(parseQueryToJSON(q)) = formatQuerySingleLine(q)
FROM values('q String',
    'EXPLAIN TEXT (SELECT 1) SETTINGS max_threads = 1',
    'SELECT 1 FORMAT TSV SETTINGS max_threads = 1');
