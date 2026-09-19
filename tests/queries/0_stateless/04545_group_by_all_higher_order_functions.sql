-- Higher-order functions (arrayMap/arrayFilter/...) with GROUP BY ALL.
-- https://github.com/ClickHouse/ClickHouse/issues/56019
-- https://github.com/ClickHouse/ClickHouse/issues/111090
-- The lambda argument (and its parameters) must not be collected as a GROUP BY key, while free
-- columns referenced in the lambda body still become keys. Checked for both analyzers.

SET enable_analyzer = 1;

-- #56019: aggregate in a non-lambda argument.
SELECT 'bar' AS foo, arrayFilter(x -> x > 0, groupArray(number)) FROM (SELECT number FROM numbers(5)) GROUP BY ALL;
SELECT arrayMap(x -> x + 1, groupArray(number)) FROM (SELECT number FROM numbers(5)) GROUP BY ALL;

-- #111090: aggregate inside the lambda body.
SELECT arrayMap(x -> first_value(_Start), range(0, 11)) FROM (SELECT 77 AS _Start) GROUP BY ALL;
SELECT arrayMap(x -> first_value(_Start), range(0, first_value(_Count))) FROM (SELECT 77 AS _Start, 11 AS _Count) GROUP BY ALL;
SELECT arrayMap(x -> first_value(_Start), range(0, 11)) FROM (SELECT 77 AS _Start) GROUP BY ();

-- Higher-order-function family, aggregate in a non-lambda argument.
SELECT arrayExists(x -> x > 2, groupArray(number)) FROM numbers(5) GROUP BY ALL;
SELECT arrayCount(x -> x > 2, groupArray(number)) FROM numbers(5) GROUP BY ALL;
SELECT arraySort(x -> -x, groupArray(number)) FROM numbers(5) GROUP BY ALL;
SELECT arrayFirst(x -> x > 2, groupArray(number)) FROM numbers(5) GROUP BY ALL;

-- Higher-order-function family, aggregate inside the lambda body.
SELECT arrayExists(x -> sum(number) > x, [1, 2, 3]) FROM numbers(5) GROUP BY ALL;
SELECT arrayMap(x -> x + sum(number), [1, 2, 3]) FROM numbers(5) GROUP BY ALL;
SELECT arrayCount(x -> x < max(number), [1, 2, 3]) FROM numbers(5) GROUP BY ALL;

-- A free column referenced only inside the lambda body still becomes a GROUP BY key, matching an
-- explicit GROUP BY of that column; the lambda parameter must not.
SELECT arrayMap(x -> x + c + sum(v), arr) FROM (SELECT 5 AS c, [1, 2] AS arr, 3 AS v) GROUP BY ALL;
SELECT k, arrayMap(x -> first_value(v), range(0, 3)) FROM (SELECT number % 2 AS k, number AS v FROM numbers(6)) GROUP BY ALL ORDER BY k;

-- Nested lambda: inner body references the outer lambda parameter, a free column and an aggregate.
SELECT arrayMap(x -> arrayMap(y -> y + x + c + sum(v), [1]), arr) FROM (SELECT 5 AS c, [9] AS arr, 2 AS v) GROUP BY ALL;
-- Nested lambda with the aggregate outside the inner lambda: the inner higher-order function
-- references the outer parameter, so it must not become a key; only the free column does.
SELECT arrayMap(x -> arrayMap(y -> y + x, [1]) || [toUInt8(sum(v))], arr) FROM (SELECT [10, 20] AS arr, 3 AS v) GROUP BY ALL;

-- Type wrappers.
SELECT arrayFilter(x -> x > 0, groupArray(toNullable(number))) FROM numbers(5) GROUP BY ALL;
SELECT arrayMap(x -> first_value(toLowCardinality(_Start)), range(0, 3)) FROM (SELECT 7 AS _Start) GROUP BY ALL;

-- Controls: no aggregate keeps the whole higher-order function as a single key; the lambda
-- parameter alone is never a key.
SELECT arrayMap(x -> x + 1, [1, 2, 3]) FROM numbers(2) GROUP BY ALL;
SELECT arrayMap(x -> x, arr), sum(v) FROM (SELECT [1, 2] AS arr, 3 AS v) GROUP BY ALL;

SET enable_analyzer = 0;

-- #56019: aggregate in a non-lambda argument.
SELECT 'bar' AS foo, arrayFilter(x -> x > 0, groupArray(number)) FROM (SELECT number FROM numbers(5)) GROUP BY ALL;
SELECT arrayMap(x -> x + 1, groupArray(number)) FROM (SELECT number FROM numbers(5)) GROUP BY ALL;

-- #111090: aggregate inside the lambda body.
SELECT arrayMap(x -> first_value(_Start), range(0, 11)) FROM (SELECT 77 AS _Start) GROUP BY ALL;
SELECT arrayMap(x -> first_value(_Start), range(0, first_value(_Count))) FROM (SELECT 77 AS _Start, 11 AS _Count) GROUP BY ALL;
SELECT arrayMap(x -> first_value(_Start), range(0, 11)) FROM (SELECT 77 AS _Start) GROUP BY ();

-- Higher-order-function family, aggregate in a non-lambda argument.
SELECT arrayExists(x -> x > 2, groupArray(number)) FROM numbers(5) GROUP BY ALL;
SELECT arrayCount(x -> x > 2, groupArray(number)) FROM numbers(5) GROUP BY ALL;
SELECT arraySort(x -> -x, groupArray(number)) FROM numbers(5) GROUP BY ALL;
SELECT arrayFirst(x -> x > 2, groupArray(number)) FROM numbers(5) GROUP BY ALL;

-- Higher-order-function family, aggregate inside the lambda body.
SELECT arrayExists(x -> sum(number) > x, [1, 2, 3]) FROM numbers(5) GROUP BY ALL;
SELECT arrayMap(x -> x + sum(number), [1, 2, 3]) FROM numbers(5) GROUP BY ALL;
SELECT arrayCount(x -> x < max(number), [1, 2, 3]) FROM numbers(5) GROUP BY ALL;

-- A free column referenced only inside the lambda body still becomes a GROUP BY key.
SELECT arrayMap(x -> x + c + sum(v), arr) FROM (SELECT 5 AS c, [1, 2] AS arr, 3 AS v) GROUP BY ALL;
SELECT k, arrayMap(x -> first_value(v), range(0, 3)) FROM (SELECT number % 2 AS k, number AS v FROM numbers(6)) GROUP BY ALL ORDER BY k;

-- Nested lambda.
SELECT arrayMap(x -> arrayMap(y -> y + x + c + sum(v), [1]), arr) FROM (SELECT 5 AS c, [9] AS arr, 2 AS v) GROUP BY ALL;
SELECT arrayMap(x -> arrayMap(y -> y + x, [1]) || [toUInt8(sum(v))], arr) FROM (SELECT [10, 20] AS arr, 3 AS v) GROUP BY ALL;

-- Type wrappers.
SELECT arrayFilter(x -> x > 0, groupArray(toNullable(number))) FROM numbers(5) GROUP BY ALL;
SELECT arrayMap(x -> first_value(toLowCardinality(_Start)), range(0, 3)) FROM (SELECT 7 AS _Start) GROUP BY ALL;

-- Controls.
SELECT arrayMap(x -> x + 1, [1, 2, 3]) FROM numbers(2) GROUP BY ALL;
SELECT arrayMap(x -> x, arr), sum(v) FROM (SELECT [1, 2] AS arr, 3 AS v) GROUP BY ALL;
