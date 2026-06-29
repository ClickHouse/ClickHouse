-- Exercise higher-order array functions and their captures to cover
-- ColumnFunction / lambda closure handling (Columns/ColumnFunction.cpp).

SELECT '--- arrayMap: basic and multi-array ---';
SELECT arrayMap(x -> x * 2, [1, 2, 3]);
SELECT arrayMap((x, y) -> x + y, [1, 2, 3], [10, 20, 30]);
SELECT arrayMap((x, y, z) -> x * y + z, [1, 2], [10, 20], [100, 200]);

SELECT '--- arrayFilter / arrayExists / arrayAll ---';
SELECT arrayFilter(x -> x > 1, [1, 2, 3]);
SELECT arrayExists(x -> x > 5, [1, 2, 3]);
SELECT arrayExists(x -> x > 2, [1, 2, 3]);
SELECT arrayAll(x -> x > 0, [1, 2, 3]);
SELECT arrayAll(x -> x > 1, [1, 2, 3]);

SELECT '--- arrayCount / arraySum / arrayAvg / arrayMin / arrayMax ---';
SELECT arrayCount(x -> x > 1, [1, 2, 3]);
SELECT arraySum(x -> x * 2, [1, 2, 3]);
SELECT arrayAvg(x -> x * 2, [1, 2, 3]);
SELECT arrayMin(x -> x * -1, [1, 2, 3]);
SELECT arrayMax(x -> x * -1, [1, 2, 3]);

SELECT '--- arrayFirst / arrayLast / arrayFirstIndex / arrayLastIndex ---';
SELECT arrayFirst(x -> x > 1, [1, 2, 3]);
SELECT arrayLast(x -> x < 3, [1, 2, 3]);
SELECT arrayFirstIndex(x -> x > 1, [1, 2, 3]);
SELECT arrayLastIndex(x -> x < 3, [1, 2, 3]);

SELECT '--- arrayReduce (named aggregate, no lambda) ---';
SELECT arrayReduce('sum', [1, 2, 3]);
SELECT arrayReduce('avg', [1, 2, 3]);
SELECT arrayReduce('uniq', [1, 2, 2, 3, 3, 3]);

SELECT '--- arrayFold with explicit accumulator type (avoid type mismatch) ---';
SELECT arrayFold((acc, x) -> acc + x::UInt64, [1, 2, 3]::Array(UInt64), 0::UInt64);
SELECT arrayFold((acc, x) -> concat(acc, toString(x)), [1, 2, 3]::Array(UInt64), ''::String);

SELECT '--- captured outer variable ---';
SELECT arrayMap(x -> x + factor, [1, 2, 3]) FROM (SELECT 100 AS factor);

SELECT '--- lambda used in different operators ---';
SELECT arraySort(x -> -x, [3, 1, 4, 1, 5, 9, 2, 6]);
SELECT arrayReverseSort(x -> x, [3, 1, 4, 1, 5, 9]);

SELECT '--- arraySplit / arrayPartialSort ---';
SELECT arraySplit(x -> x > 2, [1, 2, 3, 4, 5]);

SELECT '--- nested higher-order: arrayMap over Array(Array(Int)) ---';
SELECT arrayMap(row -> arraySum(x -> x * 2, row), [[1, 2], [3, 4], [5]]);

SELECT '--- arrayMap returning array changes result type ---';
SELECT arrayMap(x -> [x, x * 2], [1, 2, 3]);

SELECT '--- arrayMap returning tuples ---';
SELECT arrayMap(x -> (x, toString(x)), [1, 2, 3]);

SELECT '--- arrayFilter with tuple input ---';
SELECT arrayFilter((x, y) -> x > y, [1, 5, 3], [2, 2, 4]);

SELECT '--- arrayMap inside aggregates (sum of map results) ---';
SELECT sum(arraySum(arrayMap(x -> x + 1, arr)))
FROM (SELECT arrayJoin([[1, 2], [3, 4]]) AS arr);

SELECT '--- arrayFilter in WHERE ---';
SELECT arr FROM (
    SELECT [1, 2, 3] AS arr UNION ALL SELECT [4, 5, 6] UNION ALL SELECT [7, 8, 9])
WHERE length(arrayFilter(x -> x > 5, arr)) > 0
ORDER BY arr;

SELECT '--- Errors: lambda with wrong number of arguments ---';
SELECT arrayMap((x, y) -> x + y, [1, 2, 3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- lambda returning incompatible type to fold accumulator ---';
SELECT arrayFold((acc, x) -> acc + x, [1, 2, 3], 0); -- { serverError TYPE_MISMATCH }
