-- A numeric literal keeps its original text until the type it is used with is known. The AST JSON
-- encoding must carry that text, so `formatQueryFromJSON` reproduces the same literal and the
-- `clickhouse_json` dialect runs the same query as the SQL text does.

SET allow_experimental_json_type = 0; -- avoid interference with JSON column type

SELECT 'field' AS t,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'value', 'field_type') AS ft,
    JSONExtractString(j, 'list_of_selects', 'children', 1, 'select', 'children', 1, 'value', 'value') AS val
FROM (SELECT parseQueryToJSON('SELECT 1.123456789012345679') AS j);

-- The exact literal survives the round-trip: the formatted query is the query that went in, so the
-- comparison keeps the same exact semantics instead of falling back to rounded `Float64` values.
WITH 'SELECT CAST(\'1.123456789012345678\', \'Decimal128(18)\') = 1.123456789012345679' AS q
SELECT 'roundtrip' AS t, formatQueryFromJSON(parseQueryToJSON(q)) = q AS identical;

SELECT 'exact' AS t, CAST('1.123456789012345678', 'Decimal128(18)') = 1.123456789012345679 AS equal;

SELECT 'forms' AS t, formatQueryFromJSON(parseQueryToJSON(q)) AS formatted
FROM values('q String', 'SELECT 1.20', 'SELECT 1e5', 'SELECT 0x1p4', 'SELECT -1.5',
    'SELECT 123456789012345678901234567890', 'SELECT [1.5, 2.5]', 'SELECT nan')
ORDER BY q;

-- A `NumberLiteral` is formatted back into the query verbatim, so text that is not a numeric
-- literal must be rejected instead of being carried into the formatted query as-is.
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"Number","value":"1 UNION ALL SELECT 42"}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"Number","value":"Number_1"}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"Number","value":""}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"Number","value":1.5}}]}}]}}'); -- { serverError BAD_ARGUMENTS }
