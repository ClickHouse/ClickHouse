-- A bare Array-typed column on the right side of IN is the set of its elements,
-- so `x IN arr` must behave like `has(arr, x)`, exactly like an array literal or an
-- array-returning function on the right side of IN. Previously the column was wrapped
-- in tuple() and treated as a single set element, giving a wrong (always-false) result
-- for stringifiable elements, or an error for non-stringifiable ones.

SET enable_analyzer = 1;

-- String array column (matches the reported case with `has(tables, 'system.tables')`).
SELECT 'a' IN arr AS in_res, has(arr, 'a') AS has_res
FROM (SELECT ['a', 'b'] AS arr UNION ALL SELECT ['c', 'd'] AS arr)
ORDER BY arr[1];

-- Numeric array column (used to throw ILLEGAL_TYPE_OF_ARGUMENT).
SELECT 2 IN arr AS in_res, has(arr, 2) AS has_res
FROM (SELECT [1, 2, 3] AS arr UNION ALL SELECT [4, 5, 6] AS arr)
ORDER BY arr[1];

-- NOT IN must be the negation.
SELECT 2 NOT IN arr AS not_in_res, NOT has(arr, 2) AS not_has_res
FROM (SELECT [1, 2, 3] AS arr UNION ALL SELECT [4, 5, 6] AS arr)
ORDER BY arr[1];

-- The left argument may itself be a column.
SELECT x IN arr AS in_res, has(arr, x) AS has_res
FROM (SELECT 2 AS x, [1, 2, 3] AS arr UNION ALL SELECT 9 AS x, [1, 2, 3] AS arr)
ORDER BY x;

-- A materialized (non-constant) array column.
SELECT 2 IN arr AS in_res, has(arr, 2) AS has_res
FROM (SELECT materialize([1, 2, 3]) AS arr);

-- The rewrite only fires when the array is exactly one dimension deeper than the left argument.
-- `Array(T)` on the left with `Array(Array(T))` on the right (depth + 1) -> has().
SELECT a IN b AS in_res, has(b, a) AS has_res
FROM (SELECT [1, 2] AS a, [[1, 2], [3, 4]] AS b UNION ALL SELECT [9] AS a, [[1, 2]] AS b)
ORDER BY a[1];

-- Equal depths (`Array(T)` on both sides) are a one-element set, i.e. equality, not has().
SELECT a IN b AS in_res, a = b AS eq_res
FROM (SELECT ['x'] AS a, ['x'] AS b UNION ALL SELECT ['x'] AS a, ['y'] AS b)
ORDER BY b[1]
SETTINGS transform_null_in = 1;

-- LowCardinality: `LowCardinality(Array(...))` itself cannot exist - `DataTypeLowCardinality`
-- rejects `Array` as its dictionary type regardless of `allow_suspicious_low_cardinality_types` -
-- so the LowCardinality shapes that can meet this rewrite are a LowCardinality left argument and
-- LowCardinality array elements on the right. Both must go through has() like their
-- full-cardinality counterparts.
SELECT x IN arr AS in_res, has(arr, x) AS has_res
FROM (SELECT 'a'::LowCardinality(String) AS x, ['a', 'b'] AS arr UNION ALL SELECT 'z'::LowCardinality(String) AS x, ['a', 'b'] AS arr)
ORDER BY x;

SELECT x IN arr AS in_res, has(arr, x) AS has_res
FROM (SELECT 2::LowCardinality(UInt8) AS x, [1, 2, 3] AS arr UNION ALL SELECT 9::LowCardinality(UInt8) AS x, [1, 2, 3] AS arr)
ORDER BY x
SETTINGS allow_suspicious_low_cardinality_types = 1;

SELECT 'a' IN arr AS in_res, has(arr, 'a') AS has_res
FROM (SELECT ['a', 'b']::Array(LowCardinality(String)) AS arr UNION ALL SELECT ['c', 'd']::Array(LowCardinality(String)) AS arr)
ORDER BY arr[1];

-- Creating LowCardinality(Array(...)) must keep throwing (guards the premise above).
SELECT CAST([1, 2, 3] AS LowCardinality(Array(UInt8))) SETTINGS allow_suspicious_low_cardinality_types = 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Regression guard: a scalar (non-array) column on the right stays a one-element set (`x = col`).
SELECT a IN b AS in_res, a = b AS eq_res
FROM (SELECT 1 AS a, 1 AS b UNION ALL SELECT 1 AS a, 2 AS b)
ORDER BY b;

-- Regression guard: array literal on the right still works.
SELECT 2 IN [1, 2, 3], 5 IN [1, 2, 3];
