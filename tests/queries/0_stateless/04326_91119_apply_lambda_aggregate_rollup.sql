-- https://github.com/ClickHouse/ClickHouse/issues/91119
--
-- Regression coverage for the analyzer change in PR #104350. When walking
-- enclosing scopes to apply `group_by_use_nulls` Nullable wrapping, the
-- aggregate-function suppression must remain OR-accumulated across the chain
-- of LAMBDA scopes up to (and including) the enclosing `QUERY` scope. A
-- per-scope check breaks this for `* APPLY x -> agg(x, key)` queries: the
-- LAMBDA scope sees the aggregate on its stack but its own
-- `nullable_group_by_keys` is empty; the outer `QUERY` scope holds the keys
-- but its own stack does not see the aggregate (the aggregate lives inside
-- the lambda body), so a per-scope check would wrap the inner reference
-- as `Nullable` and the aggregate's per-group input column type would no
-- longer match the function's signature, causing an exception (or, for some
-- aggregates, a memory access fault inside `addBatchSinglePlace`).
--
-- The cases below all combine `group_by_use_nulls = 1` with `* APPLY <lambda>`
-- where the lambda body contains an aggregate that references a GROUP BY key,
-- across the four GROUP BY shapes (`ROLLUP`, `CUBE`, `GROUPING SETS`, plain).
-- Each must run to completion and produce the expected output.

SET enable_analyzer = 1;
SET group_by_use_nulls = 1;

SELECT '-- GROUPING SETS, materialize key, argMax ---';
SELECT * APPLY x -> argMax(x, number) FROM numbers(1) GROUP BY GROUPING SETS ((materialize(65537)), (*));

SELECT '-- ROLLUP, argMax ---';
SELECT * APPLY x -> argMax(x, number) FROM numbers(1) GROUP BY number WITH ROLLUP;

SELECT '-- CUBE, argMax ---';
SELECT * APPLY x -> argMax(x, number) FROM numbers(1) GROUP BY number WITH CUBE;

SELECT '-- ROLLUP, sum ---';
SELECT * APPLY x -> sum(x) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY number;

SELECT '-- ROLLUP, function-form aggregate ---';
SELECT * APPLY sum FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY number;

SELECT '-- ROLLUP, regex-filtered APPLY ---';
SELECT (SELECT * APPLY (x -> argMax(x, number), 'f_'))
FROM numbers(256) GROUP BY * WITH ROLLUP WITH TOTALS LIMIT -9223372036854775807, 225;

SELECT '-- nested aggregate inside a function ---';
SELECT * APPLY x -> toString(argMax(x, number))
FROM numbers(1) GROUP BY GROUPING SETS ((materialize(65537)), (*));

SELECT '-- deeply nested aggregate ---';
SELECT * APPLY x -> length(toString(sum(x)))
FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY number;

-- Sanity: a non-aggregate APPLY must still produce a `Nullable` projection
-- after `WITH ROLLUP` so the suppression is genuinely scoped to the aggregate
-- argument path and not a blanket disable.
SELECT '-- non-aggregate APPLY remains Nullable ---';
SELECT * APPLY toString FROM (SELECT number FROM numbers(2)) GROUP BY number WITH ROLLUP ORDER BY number;
