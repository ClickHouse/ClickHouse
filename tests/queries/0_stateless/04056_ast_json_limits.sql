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
