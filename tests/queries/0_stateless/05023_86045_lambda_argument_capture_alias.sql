-- An expression bound to an alias outside of a lambda must not see the lambda arguments,
-- even when an argument has the same name as an identifier of that expression.

SET enable_analyzer = 1;

-- The original report: the aggregate argument inside `steps` was captured by the lambda argument `step`.
SELECT
    groupArray(step) AS steps,
    arrayMin(arrayFilter((time, step) -> (step = 1), steps, steps)) AS step1,
    arrayFilter((time, step) -> (time >= step1), steps, steps) AS step2
FROM
(
    SELECT 1 AS step
);

SELECT
    groupArray(step) AS steps,
    arrayFilter(x -> (x = 1), steps) AS a,
    arrayFilter(step -> (step >= a[1]), steps) AS b
FROM
(
    SELECT 1 AS step
);

-- Silently wrong results: `n` is `number + 1` of the table, not of the lambda argument.
SELECT number + 1 AS n, arrayMap(number -> number + n, [1, 2]) FROM numbers(2);

WITH number + 1 AS n SELECT arrayMap(number -> number + n, [1, 2]) FROM numbers(2);

-- The alias belongs to the outer lambda, so it refers to the argument of the outer lambda.
SELECT arrayMap(x -> (x * 2 AS d) + arrayMap(y -> y + d, [10])[1], [1, 2]);

-- The same for an alias to a bare identifier.
SELECT id AS id2, arrayMap(id -> id + id2, [1, 2]) FROM (SELECT 5 AS id);

-- A subquery between the lambda and the scope owning the alias.
SELECT number + 1 AS n, arrayMap(number -> (SELECT n), [1, 2]) FROM numbers(2);

-- An alias that does reference the argument of the lambda it is written in still works.
SELECT arrayMap(x -> (x * 2 AS d) + d, [1, 2]);

-- A lambda argument that shadows nothing outside of the lambda stays visible to the alias:
-- naming it after an identifier of the alias is the only way to write the predicate outside.
WITH t = 'a' AS issue SELECT arrayFilter((t, t2) -> NOT issue, ['a', 'b'], [1, 2]);

-- As soon as the name does exist outside of the lambda, the alias refers to it.
WITH t = 'a' AS issue SELECT arrayFilter((t, t2) -> NOT issue, ['a', 'b'], [1, 2]) FROM (SELECT 'a' AS t);

-- An alias owned by a lambda still hides the arguments of the lambdas nested inside of it:
-- `id` in `d` is the column of the query, not the argument of the lambda that references `d`.
SELECT 5 AS id, arrayMap(x -> (x + id AS d) + arrayMap(id -> id + d, [1])[1], [1, 2]);

-- Except for the arguments of the lambda that owns the alias: `x` in `d` is the argument of the
-- outer lambda, so hiding the argument of the inner one would make the planner mix the two up,
-- because they are both named `x`. The inner argument wins - a known limitation of the planner.
SELECT arrayMap(x -> (x * 2 AS d) + arrayMap(x -> x + d, [10])[1], [1, 2]);

-- The aliased expression can bind above the scope the lambdas are nested in.
SELECT id, (WITH id + 1 AS d SELECT arrayMap(id -> id + d, [1, 2])) FROM (SELECT 5 AS id)
SETTINGS allow_experimental_correlated_subqueries = 1;

SELECT number + 1 AS n, (SELECT arrayMap(number -> number + n, [1, 2])) FROM numbers(2)
SETTINGS allow_experimental_correlated_subqueries = 1;

-- With `enable_global_with_statement = 0` an alias of an outer query is not visible in the
-- subquery, so it must not hide a lambda argument of the same name either: `x` in `d` is the
-- argument of the lambda, the only thing it can resolve to.
WITH 5 AS x SELECT (WITH x + 1 AS d SELECT arrayMap(x -> x + d, [1, 2]))
SETTINGS enable_global_with_statement = 0;

-- With the setting enabled, the outer alias is visible, and the argument is hidden.
WITH 5 AS x SELECT (WITH x + 1 AS d SELECT arrayMap(x -> x + d, [1, 2]))
SETTINGS enable_global_with_statement = 1;
