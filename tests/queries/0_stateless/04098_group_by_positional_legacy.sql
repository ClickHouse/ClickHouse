-- Regression: `ParserGroupByElement` used to wrap every GROUP BY child in
-- `ASTGroupByElement`, even when no `WITH CLUSTER` modifier was present.
-- That broke the legacy (non-analyzer) positional-argument rewrite, which
-- runs directly on `groupBy()->children` and only rewrites when the node
-- itself is `ASTLiteral` — after wrapping it saw `ASTGroupByElement`
-- instead and left `GROUP BY 1` as a literal constant. The parser now
-- emits a bare expression when `WITH CLUSTER` is absent, keeping the
-- legacy shape stable.

SET enable_analyzer = 0;
SET enable_positional_arguments = 1;

SELECT number, count() FROM numbers(5) GROUP BY 1 ORDER BY number;

-- Two positional arguments mixed with explicit columns.
SELECT number, intDiv(number, 2) AS half, count()
FROM numbers(5) GROUP BY 1, 2 ORDER BY number;
