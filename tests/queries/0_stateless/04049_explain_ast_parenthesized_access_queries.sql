-- Regression test: EXPLAIN AST with trailing SETTINGS must not produce
-- unparseable parenthesized output for access control queries.

-- The formatter wraps non-ASTQueryWithOutput inner queries in parens when
-- trailing output options exist. The parser must accept that form back.
SELECT formatQuery('EXPLAIN AST graph = 1, optimize = 0 ALTER ROW POLICY p1 ON d0.`t57` RENAME TO p7 SETTINGS schema_inference_use_cache_for_s3 = 0');

-- Verify the parenthesized form can also be parsed directly.
SELECT formatQuery('EXPLAIN AST graph = 1, optimize = 0 (ALTER ROW POLICY p1 ON d0.t57 RENAME TO p7) SETTINGS schema_inference_use_cache_for_s3 = 0');
