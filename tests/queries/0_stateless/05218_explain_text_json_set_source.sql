SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN TEXT","query":{"type":"SetQuery","changes":[{"name":"max_threads","value":{"field_type":"UInt64","value":1}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN TEXT","query":{"type":"SetQuery","is_standalone":true}}'); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(parseQueryToJSON(q)) = formatQuerySingleLine(q)
FROM values('q String',
    'EXPLAIN TEXT (SET max_threads = 1)',
    'EXPLAIN TEXT (SET max_threads = DEFAULT)',
    'EXPLAIN TEXT (SET param_x = ''1'')');
