SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN TEXT","query":{"type":"Literal","value":{"field_type":"UInt64","value":1}}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN TEXT","query":{"type":"Identifier","name":"x"}}'); -- { serverError BAD_ARGUMENTS }

WITH 'EXPLAIN TEXT (SELECT 1) ONELINE' AS q
SELECT formatQueryFromJSON(parseQueryToJSON(q)) = formatQuerySingleLine(q);
