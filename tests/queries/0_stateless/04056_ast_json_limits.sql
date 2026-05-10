-- Test that parseQueryToJSON respects parser limits and
-- formatQueryFromJSON enforces AST size/depth limits.

-- parseQueryToJSON should respect max_parser_depth.
-- Use a value high enough for the outer query to parse, but low enough
-- for the deeply-nested inner query to fail.
SET max_parser_depth = 50;
SELECT parseQueryToJSON('SELECT ((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((1))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))'); -- { serverError TOO_DEEP_RECURSION }

-- Reset to default.
SET max_parser_depth = 1000;

-- formatQueryFromJSON should enforce max_ast_depth.
-- Use a limit high enough for the outer query (depth ~8) but low enough
-- for the reconstructed AST of 1+2+3+4+5 (depth ~12) to exceed it.
SET max_ast_depth = 10;
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 + 2 + 3 + 4 + 5')); -- { serverError TOO_DEEP_AST }

-- Reset to default.
SET max_ast_depth = 1000;

-- formatQueryFromJSON should enforce max_ast_elements.
-- The outer query has ~8 AST elements, so use a limit above that
-- but below the count of the reconstructed AST (which has many columns and table references).
SET max_ast_elements = 15;
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a, b, c, d, e, f, g, h, i, j, k, l FROM t')); -- { serverError TOO_BIG_AST }

-- Reset to default.
SET max_ast_elements = 50000;

-- Malformed JSON should produce a clear error.
SELECT formatQueryFromJSON('not json'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"NoSuchASTNode"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"ExpressionList","children":[null]}'); -- { serverError BAD_ARGUMENTS }

-- ASTOrderByElement: only direction values -1 and 1 are valid; reject others.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT a FROM t ORDER BY a ASC'), '"direction":1', '"direction":0')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT a FROM t ORDER BY a ASC'), '"nulls_direction":1', '"nulls_direction":2')); -- { serverError BAD_ARGUMENTS }

-- Field value must be a JSON object: malformed shape (e.g. string instead of object) must be rejected
-- instead of silently becoming NULL.
SELECT formatQueryFromJSON('{"type":"Literal","value":"oops"}'); -- { serverError BAD_ARGUMENTS }

-- A `value` payload routed through `Field::restoreFromDump` (i.e. a `field_type` that is not one of
-- the explicitly-handled scalar/Array/Tuple/Map cases) must respect `max_ast_depth`. Otherwise a
-- shallow JSON object could smuggle a deeply nested `Array_[Array_[...]]` dump and exhaust the stack.
SET max_ast_depth = 8;
SELECT formatQueryFromJSON('{"type":"Literal","value":{"field_type":"AggregateFunctionState","value":"Array_[Array_[Array_[Array_[Array_[Array_[Array_[Array_[Array_[Array_[Array_[Array_[1]]]]]]]]]]]]"}}'); -- { serverError BAD_ARGUMENTS }
SET max_ast_depth = 1000;
