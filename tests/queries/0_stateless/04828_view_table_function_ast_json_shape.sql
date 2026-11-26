-- The parser produces a bare `SelectWithUnionQuery` argument of `view` only for the table function
-- form `view(SELECT ...)`: `ViewLayer` pushes the select and the closing bracket must follow, so the
-- shape is always exactly one select without parameters. `ASTFunction::formatImplWithoutAlias` prints
-- the query-argument form (which drops parameters and is parseable only by the table function parser)
-- for a single bare select, so `ASTFunction::readJSON` must reject any other combination that contains
-- a bare select, which the parser cannot produce and which would otherwise format to unparseable text.
-- Note that in an expression context `view` parses as an ordinary function (e.g. `view(1, 2)`), so
-- shapes without a bare select stay accepted.

SET enable_json_ast_dialect = 1;

-- Positives: both parser-produced forms round-trip.

SELECT formatQueryFromJSON(parseQueryToJSON($$SELECT * FROM view(SELECT 1)$$))
    = formatQuerySingleLine($$SELECT * FROM view(SELECT 1)$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$SELECT view(1, 2)$$))
    = formatQuerySingleLine($$SELECT view(1, 2)$$);

-- Positive handcrafted payloads: the table function shape (a single select), and the expression
-- shape without a bare select.

SELECT formatQueryFromJSON('{"type":"Function","name":"view","arguments":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}]}}');

SELECT formatQueryFromJSON('{"type":"Function","name":"view","arguments":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}},{"type":"Literal","value":{"field_type":"UInt64","value":2}}]}}');

-- Negatives: a bare select next to another argument is parser-impossible and is rejected.

SELECT formatQueryFromJSON('{"type":"Function","name":"view","arguments":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}},{"type":"Literal","value":{"field_type":"UInt64","value":2}}]}}'); -- { serverError BAD_ARGUMENTS }

-- A bare select in the wrong position.

SELECT formatQueryFromJSON('{"type":"Function","name":"view","arguments":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":2}},{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}]}}'); -- { serverError BAD_ARGUMENTS }

-- The table function shape with parameters (the parser never produces a parameterized `view` table
-- function; the query-argument formatting path would silently drop the parameters).

SELECT formatQueryFromJSON('{"type":"Function","name":"view","parameters":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]},"arguments":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}}]}}'); -- { serverError BAD_ARGUMENTS }

-- The same malformation nested in a real query is rejected as well. Assert the anchor first, so a
-- change to the serialization fails here instead of silently weakening the check below into a no-op.

SELECT position(parseQueryToJSON($$SELECT * FROM view(SELECT 1)$$),
    '"type":"Function","name":"view","arguments"') > 0;

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$SELECT * FROM view(SELECT 1)$$),
    '"type":"Function","name":"view","arguments"',
    '"type":"Function","name":"view","parameters":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]},"arguments"')); -- { serverError BAD_ARGUMENTS }

-- Non-canonical spellings of the name. The parser dispatches to the table function parser on the
-- lowercased name but always produces the canonical spelling `view` (`ViewLayer`), and execution
-- matches the name case-sensitively (e.g. `StorageView::replaceWithSubquery`), so the name is
-- canonicalized to `view` during deserialization, the same way the parser does.

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$SELECT * FROM view(SELECT 1)$$),
    '"type":"Function","name":"view","arguments"',
    '"type":"Function","name":"VIEW","arguments"'));

SELECT formatQuerySingleLine(formatQueryFromJSON(replace(parseQueryToJSON($$SELECT * FROM view(SELECT 1)$$),
    '"type":"Function","name":"view","arguments"',
    '"type":"Function","name":"VIEW","arguments"')))
    = formatQuerySingleLine($$SELECT * FROM view(SELECT 1)$$);

-- A parser-impossible bare-select shape is rejected for a non-canonical spelling as well.

SELECT formatQueryFromJSON('{"type":"Function","name":"VIEW","arguments":{"type":"ExpressionList","children":[{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]}},{"type":"Literal","value":{"field_type":"UInt64","value":2}}]}}'); -- { serverError BAD_ARGUMENTS }
