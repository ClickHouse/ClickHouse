-- A parametrised alias is preserved as written, whether or not the parameter has a value.
EXPLAIN TEXT (SELECT 1 AS {name:Identifier}) ONELINE;
EXPLAIN TEXT (WITH 1 AS {name:Identifier} SELECT {name:Identifier}) ONELINE;
SET param_name = 'x';
EXPLAIN TEXT (SELECT 1 AS {name:Identifier}) ONELINE;
SELECT formatQuerySingleLine('SELECT 1 AS {name:Identifier}');
-- Actions refuse a parametrised alias exactly like a plain one. The SQL parser drops a parametrised
-- alias inside parentheses before the action sees it, so the operand is built through JSON.
SELECT formatQueryFromJSON('{"type":"ExplainQuery","kind":"EXPLAIN TEXT","query":{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]},"limit_length":{"type":"Literal","value":{"field_type":"UInt64","value":3}}},"actions":{"type":"ExpressionList","children":[{"type":"ExplainTextAction","kind":"MODIFY LIMIT","operand":{"type":"Literal","value":{"field_type":"UInt64","value":1},"parametrised_alias":{"type":"QueryParameter","name":"name","param_type":"Identifier"}}}]}}'); -- { serverError BAD_ARGUMENTS }
