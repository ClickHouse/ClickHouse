-- Verify that `->` is not silently consumed when `parseLambda` fails.
-- Previously, `() -> (p0) -> isDecimalOverflow(p0)` would silently drop the
-- second `->`, causing `isDecimalOverflow` to be misinterpreted as an alias
-- and `(p0)` to become ASTSelectQuery::ALIASES, leading to an inconsistent
-- AST on roundtrip formatting.

-- Chained `->` where the left side of the second arrow is not valid lambda params:
SELECT 1, () -> (p0) -> isDecimalOverflow(p0); -- { clientError SYNTAX_ERROR }

-- Same with FROM-first syntax (original reproducer):
FROM system.one SELECT 1, () -> (p0) -> isDecimalOverflow(p0); -- { clientError SYNTAX_ERROR }

-- Invalid lambda params (literals instead of identifiers):
SELECT (1, 2) -> x; -- { clientError SYNTAX_ERROR }

-- Valid lambdas should still work:
SELECT arrayMap(x -> x + 1, [1, 2, 3]);
SELECT arrayMap((x, y) -> x + y, [1, 2], [3, 4]);
SELECT arrayMap(x -> arrayMap(y -> x + y, [10, 20]), [1, 2]);
