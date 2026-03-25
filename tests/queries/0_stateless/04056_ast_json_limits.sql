-- Test that parseQueryToJSON respects parser limits and
-- formatQueryFromJSON enforces AST size/depth limits.

-- parseQueryToJSON should respect max_parser_depth.
SET max_parser_depth = 10;
SELECT parseQueryToJSON('SELECT ((((((((((((((((((((1))))))))))))))))))))'); -- { serverError TOO_DEEP_RECURSION }

-- Reset to default.
SET max_parser_depth = 0;

-- formatQueryFromJSON should enforce max_ast_depth.
SET max_ast_depth = 2;
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 + 2 + 3 + 4 + 5')); -- { serverError TOO_DEEP_AST }

-- Reset to default.
SET max_ast_depth = 0;

-- formatQueryFromJSON should enforce max_ast_elements.
SET max_ast_elements = 3;
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT a, b, c, d, e, f, g FROM t')); -- { serverError TOO_BIG_AST }

-- Reset to default.
SET max_ast_elements = 0;

-- Malformed JSON should produce a clear error.
SELECT formatQueryFromJSON('not json'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"NoSuchASTNode"}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"Literal","children":[null]}'); -- { serverError BAD_ARGUMENTS }
