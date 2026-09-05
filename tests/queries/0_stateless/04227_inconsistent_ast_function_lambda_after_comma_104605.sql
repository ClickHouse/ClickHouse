-- Issue #104605: queries like SELECT substring(x, `x` -> `x`) aborted the
-- server with an "Inconsistent AST formatting" LOGICAL_ERROR. The parser
-- merges the comma-separated arguments into a single lambda-with-tuple
-- argument (substring((x, x) -> x)) -- this is the documented
-- f(a, b -> body) == f((a, b) -> body) sugar used by `mapApply`, `arrayFold`,
-- etc. -- which the formatter emitted faithfully, but the `SubstringLayer`
-- parser rejected the formatted output at state 0 because it did not accept
-- a closing bracket there, so the format/re-parse round-trip check aborted
-- the server.
--
-- The fix accepts a closing bracket at state 0 in `SubstringLayer` and
-- `PositionLayer` ONLY when a lambda operator is pending. This lets the
-- formatter's one-argument-with-merged-tuple-lambda output round-trip while
-- continuing to reject bare one-argument forms (`substring(x)`, `position(x)`)
-- at parse time -- `02154_parser_backtracking` depends on the latter for
-- exponential-backtracking protection.

-- Original reproducer from the issue. We don't care which error fires, only
-- that the server does not abort with LOGICAL_ERROR. The error code depends
-- on the analyzer: the new analyzer resolves the lambda first and rejects
-- the duplicate parameter names (x and x) as BAD_ARGUMENTS; the old analyzer
-- rejects lambda-as-substring-argument before resolving the lambda, so the
-- duplicate-name check never fires and ILLEGAL_TYPE_OF_ARGUMENT is reported.
SELECT substring(x, `x` -> `x`); -- { serverError BAD_ARGUMENTS, ILLEGAL_TYPE_OF_ARGUMENT }

-- Variants that exercise the same SubstringLayer / PositionLayer path. With
-- distinct lambda parameter names the call reaches function resolution and
-- fails with ILLEGAL_TYPE_OF_ARGUMENT (substring / position do not accept
-- lambda arguments).
SELECT substring(s, x -> x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT position(h, x -> x); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Round-trip through formatQuerySingleLine must succeed (this is what the
-- internal AST round-trip check does, just from inside SQL).
SELECT formatQuerySingleLine('SELECT substring(x, `x` -> `x`)');
SELECT formatQuerySingleLine('SELECT position(h, `x` -> `x`)');

-- Non-trivial lambda bodies must also round-trip cleanly. The body leaves
-- higher-priority binary operators above the `lambda` operator on the
-- parser's operators stack (e.g. `[..., Lambda, Plus]` for `x -> x + 1`),
-- so the state-0 closing-bracket handler must scan the entire stack for a
-- pending lambda — not just inspect the top via `previousType`.
SELECT formatQuerySingleLine('SELECT substring(s, x -> x + 1)');
SELECT formatQuerySingleLine('SELECT position(h, x -> x + 1)');
SELECT formatQuerySingleLine('SELECT substring(a, b, x -> x AND y)');
-- Direct merged-tuple form (the AST shape the formatter actually emits)
-- must parse for both trivial and non-trivial bodies.
SELECT substring((s, x) -> x + 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT position((h, x) -> x + 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Note: bare one-argument forms `substring(x)` / `position(x)` continue to
-- fail at parse time with `SYNTAX_ERROR`. That is exercised by
-- `02154_parser_backtracking` (deeply-nested `position(x)`,
-- `position(x y)`, `position(x IN y)`); not re-asserted here because the
-- test runner aborts a multi-query batch on the first `SYNTAX_ERROR`.

-- Legitimate substring / position calls must keep working.
SELECT substring('abcdef', 2, 3);
SELECT substring('abcdef' FROM 2 FOR 3);
SELECT position('abcdef', 'cd');
SELECT position('cd' IN 'abcdef');

-- Legitimate higher-order function lambdas must keep working -- this is the
-- documented f(a, b -> body) == f((a, b) -> body) merging behavior used by
-- mapApply, arrayFold, etc.
SELECT arrayMap(x -> x + 1, [1, 2, 3]);
SELECT arrayMap((x, y) -> x + y, [1, 2, 3], [10, 20, 30]);
SELECT arrayFilter(x -> x > 1, [1, 2, 3]);
SELECT arrayFold(acc, x -> acc + x, [1, 2, 3, 4], toUInt64(0));
SELECT mapApply((k, v) -> (k, v * 2), map(1, 10, 2, 20));
SELECT mapApply(k, v -> (k, v * 2), map(1, 10, 2, 20));
