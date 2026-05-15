-- Regression test for `Inconsistent AST formatting` between two `ExpressionList`
-- nodes (STID 1941-1bfa) when an aliased `IN` appears as the right-hand side
-- of an outer `IN` that is itself inside a multi-argument function call.
--
-- For an input like `f(a, X IN ((Y IN (...)) AS alias))`, the outer `IN`
-- parses with `parenthesized=false` and the formatter adds `(...)` around it
-- via `need_parens_around_in` (the IN sits inside a multi-arg function call,
-- so `frame.current_function != nullptr`). On re-parse those parens set
-- `parenthesized=true` on the outer `IN`; on the second format
-- `IAST::format` emits the parens via the `parenthesized` path and resets
-- `frame.current_function = nullptr` for descendants. The inner `IN` then no
-- longer sees `in_function_args == true` and stops emitting its own `(...)`,
-- so the inner `(Y IN (...))` parens disappear and the second format differs
-- from the first.
--
-- Fix: when the outer `IN`'s `formatImplWithoutAlias` itself emits the
-- wrapping `(...)` via `need_parens_around_in`, also clear `current_function`
-- for descendants — so the two paths produce the same output.

SELECT formatQuerySingleLine('SELECT g(1, 2 IN ((3 IN (4, 5)) AS x))');
SELECT formatQuerySingleLine('SELECT concatAssumeInjective((1, 2 IN ((3 IN (4, 5)) AS x)), 6)');
SELECT formatQuerySingleLine('SELECT g(1, 2 GLOBAL NOT IN ((3 IN (4, 5)) AS x))');
SELECT formatQuerySingleLine('SELECT g(1, 2 NOT IN ((3 GLOBAL IN (4, 5)) AS x))');

-- The single-arg case (no `current_function`) was already stable; keep it
-- here so future refactors do not silently break it.
SELECT formatQuerySingleLine('SELECT 1 IN ((2 IN (3, 4)) AS x)');
