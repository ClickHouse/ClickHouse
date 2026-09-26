-- Regression test for `Inconsistent AST formatting` between two `ExpressionList`
-- nodes when a subquery in a column-list position carries the `parenthesized`
-- flag. Inputs like `(+(SELECT ...))` parse to an `ASTSubquery` with
-- `parenthesized=true` (the unary `+` is absorbed and the outer parens flag
-- the inner Subquery). `ASTSubquery::formatImplWithoutAlias` always emits its
-- own `(SELECT ...)` parens; emitting the `parenthesized` flag's parens on top
-- produced `((SELECT ...))`, which the parser collapses back to a single pair,
-- breaking the format-parse-format round-trip.

SELECT formatQuerySingleLine('SELECT (+(SELECT 1))');
SELECT formatQuerySingleLine('SELECT 1, (+(SELECT 1)), 2');
SELECT formatQuerySingleLine('SELECT 1 INTERSECT ALL (SELECT 5, (+(SELECT 1 OFFSET 0.5 SETTINGS use_skip_indexes = 0)), (dictGetUInt64(''d'', ''c'', 0)) AS a3 FROM (SELECT 1) AS t)');
