-- The parser produces a bare `SelectWithUnionQuery` argument of `viewIfPermitted` only for the table
-- function form `viewIfPermitted(SELECT ... ELSE table_function(...))`: `ViewLayer` pushes the select,
-- and after `ELSE` only a function call is accepted, so the shape is always exactly (select, function)
-- without parameters. `ASTFunction::formatImplWithoutAlias` prints the `ELSE` form (parseable only by
-- the table function parser) for exactly that shape, so `ASTFunction::readJSON` must reject any other
-- combination that contains a bare select, which the parser cannot produce and which would otherwise
-- format to unparseable text. Note that in an expression context `viewIfPermitted` parses as an
-- ordinary function (e.g. `viewIfPermitted(1, 2)`), so shapes without a bare select stay accepted.

SET enable_json_ast_dialect = 1;

-- Positives: both parser-produced forms round-trip.

SELECT formatQueryFromJSON(parseQueryToJSON($$SELECT * FROM viewIfPermitted(SELECT 1 ELSE null('x UInt8'))$$))
    = formatQuerySingleLine($$SELECT * FROM viewIfPermitted(SELECT 1 ELSE null('x UInt8'))$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$SELECT viewIfPermitted(1, 2)$$))
    = formatQuerySingleLine($$SELECT viewIfPermitted(1, 2)$$);

-- Positive handcrafted payloads: the table function shape (select, function), and the expression
-- shape without a bare select.

SELECT formatQueryFromJSON('{"type":"Function","name":"viewIfPermitted","arguments":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}},{"type":"Function","name":"null","arguments":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"String","value":"x UInt8"}}]}}]}}');

SELECT formatQueryFromJSON('{"type":"Function","name":"viewIfPermitted","arguments":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}},{"type":"Literal","value":{"field_type":"UInt64","value":2}}]}}');

-- Negatives: a bare select paired with a non-function is parser-impossible and is rejected.

SELECT formatQueryFromJSON('{"type":"Function","name":"viewIfPermitted","arguments":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}},{"type":"Literal","value":{"field_type":"UInt64","value":2}}]}}'); -- { serverError BAD_ARGUMENTS }

-- A bare select alone (the table function form always has the ELSE function).

SELECT formatQueryFromJSON('{"type":"Function","name":"viewIfPermitted","arguments":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}]}}'); -- { serverError BAD_ARGUMENTS }

-- A bare select in the wrong position.

SELECT formatQueryFromJSON('{"type":"Function","name":"viewIfPermitted","arguments":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":2}},{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}]}}'); -- { serverError BAD_ARGUMENTS }

-- The table function shape with parameters (the parser never produces a parameterized viewIfPermitted
-- table function).

SELECT formatQueryFromJSON('{"type":"Function","name":"viewIfPermitted","parameters":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]},"arguments":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}},{"type":"Function","name":"null","arguments":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"String","value":"x UInt8"}}]}}]}}'); -- { serverError BAD_ARGUMENTS }

-- The same malformation nested in a real query is rejected as well. Assert the anchor first, so a
-- change to the serialization fails here instead of silently weakening the check below into a no-op.

SELECT position(parseQueryToJSON($$SELECT * FROM viewIfPermitted(SELECT 1 ELSE null('x UInt8'))$$),
    '"type":"Function","name":"viewIfPermitted","arguments"') > 0;

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$SELECT * FROM viewIfPermitted(SELECT 1 ELSE null('x UInt8'))$$),
    '"type":"Function","name":"viewIfPermitted","arguments"',
    '"type":"Function","name":"viewIfPermitted","parameters":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]},"arguments"')); -- { serverError BAD_ARGUMENTS }
