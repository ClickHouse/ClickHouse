-- Exercise the many groupArray variants in AggregateFunctionGroupArray.cpp:
-- groupArray / groupArrayLast / groupArraySample / groupArrayMovingSum /
-- groupArrayMovingAvg / groupArrayInsertAt / groupArrayArray, with size limits,
-- sampler seeds, type-dispatch across numeric / String / Tuple / Decimal /
-- Nullable / Array, and the merge path through GROUP BY.

SELECT '--- groupArray basic ---';
SELECT groupArray(x) FROM (SELECT number AS x FROM numbers(5));

SELECT '--- groupArray with size limit ---';
SELECT groupArray(3)(x) FROM (SELECT number AS x FROM numbers(10));
SELECT length(groupArray(1000)(x)) FROM (SELECT number AS x FROM numbers(50));

SELECT '--- groupArrayLast: keeps trailing elements ---';
SELECT groupArrayLast(3)(x) FROM (SELECT number AS x FROM numbers(10));
SELECT length(groupArrayLast(100)(x)) FROM (SELECT number AS x FROM numbers(500));

SELECT '--- groupArraySample: deterministic with seed ---';
SELECT groupArraySample(3, 123)(x) FROM (SELECT number AS x FROM numbers(20));
SELECT groupArraySample(3, 123)(x) FROM (SELECT number AS x FROM numbers(20));
SELECT length(groupArraySample(5, 7)(x)) FROM (SELECT number AS x FROM numbers(100));

SELECT '--- groupArrayMovingSum ---';
SELECT groupArrayMovingSum(x) FROM (SELECT number AS x FROM numbers(5));
SELECT groupArrayMovingSum(3)(x) FROM (SELECT number AS x FROM numbers(6));

SELECT '--- groupArrayMovingAvg ---';
SELECT groupArrayMovingAvg(x) FROM (SELECT number AS x FROM numbers(5));
SELECT groupArrayMovingAvg(3)(x) FROM (SELECT number AS x FROM numbers(6));

SELECT '--- groupArrayInsertAt ---';
SELECT groupArrayInsertAt(x, x) FROM (SELECT number AS x FROM numbers(5));
SELECT groupArrayInsertAt('default', 3)(name, idx) FROM (
    SELECT 'a' AS name, 2::UInt32 AS idx
    UNION ALL SELECT 'b', 0::UInt32);

SELECT '--- groupArrayArray (flatten) ---';
-- UNION ALL ordering is not stable; sort for deterministic output.
SELECT arraySort(groupArrayArray(arr)) FROM (
    SELECT [1,2]::Array(Int32) AS arr
    UNION ALL SELECT [3,4]
    UNION ALL SELECT []);

SELECT '--- groupArrayIf / groupArrayOrNull ---';
SELECT groupArrayIf(x, x > 2) FROM (SELECT number AS x FROM numbers(5));
SELECT groupArrayOrNull(x) FROM (SELECT NULL AS x WHERE 0);

SELECT '--- type-dispatch: string ---';
SELECT groupArray(x) FROM (SELECT toString(number) AS x FROM numbers(3));

SELECT '--- type-dispatch: Tuple ---';
SELECT groupArray(x) FROM (SELECT (number, toString(number)) AS x FROM numbers(3));

SELECT '--- type-dispatch: Decimal ---';
SELECT groupArray(x) FROM (SELECT CAST(number AS Decimal(10,2)) AS x FROM numbers(3));

SELECT '--- type-dispatch: Nullable(Int32) ---';
SELECT groupArray(x) FROM (SELECT CAST(number AS Nullable(Int32)) AS x FROM numbers(3));

SELECT '--- merge: GROUP BY partitions ---';
SELECT p, groupArrayLast(3)(x) FROM (
    SELECT 1 AS p, number AS x FROM numbers(5)
    UNION ALL SELECT 2, number + 100 FROM numbers(5))
GROUP BY p ORDER BY p;

SELECT '--- merge with size limit + GROUP BY ---';
SELECT p, length(groupArray(3)(x)) FROM (
    SELECT number % 3 AS p, number AS x FROM numbers(30))
GROUP BY p ORDER BY p;

SELECT '--- empty input returns empty array ---';
SELECT groupArray(x) FROM (SELECT 1 AS x WHERE 0);
SELECT groupArrayLast(3)(x) FROM (SELECT 1 AS x WHERE 0);

SELECT '--- errors ---';
SELECT groupArray(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT groupArraySample(-1)(x) FROM (SELECT 1 AS x); -- { serverError BAD_ARGUMENTS }
SELECT groupArray('bad')(x) FROM (SELECT 1 AS x); -- { serverError BAD_ARGUMENTS }

-- Coverage for AggregateFunctionGroupArrayInsertAt.cpp error paths and branch guards.
-- Lines 77-78: more than two parameters → TOO_MANY_ARGUMENTS_FOR_FUNCTION
SELECT groupArrayInsertAt(0, 10, 5)(x, pos) FROM VALUES('x Int32, pos UInt32', (1, 0)); -- { serverError TOO_MANY_ARGUMENTS_FOR_FUNCTION }

-- Lines 85-87: length_to_resize (second param) exceeds 16777215 → TOO_LARGE_ARRAY_SIZE
SELECT groupArrayInsertAt(0, 16777216)(x, pos) FROM VALUES('x Int32, pos UInt32', (1, 0)); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- Lines 91-92: position argument has non-UInt type → ILLEGAL_TYPE_OF_ARGUMENT
SELECT groupArrayInsertAt(x, pos) FROM VALUES('x Int32, pos String', (1, 'a')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Lines 121-124: position value at runtime >= 16777215 → TOO_LARGE_ARRAY_SIZE
SELECT groupArrayInsertAt(x, pos) FROM VALUES('x Int32, pos UInt32', (1, 16777215)); -- { serverError TOO_LARGE_ARRAY_SIZE }

-- Lines 128-131: duplicate position — second write silently skipped; first wins.
SELECT groupArrayInsertAt(val, pos) FROM VALUES('val String, pos UInt32', ('a', 0), ('b', 0), ('c', 1));
