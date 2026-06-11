-- Tags: no-fasttest

SELECT 'array, index in range';
SELECT arrayElementOrDefault([1, 2, 3], 1, 100);
SELECT arrayElementOrDefault([1, 2, 3], 3, 100);

SELECT 'array, out of range';
SELECT arrayElementOrDefault([1, 2, 3], 4, 100);
SELECT arrayElementOrDefault([1, 2, 3], 0, 100);

SELECT 'negative index';
SELECT arrayElementOrDefault([1, 2, 3], -1, 100);
SELECT arrayElementOrDefault([1, 2, 3], -3, 100);
SELECT arrayElementOrDefault([1, 2, 3], -4, 100);

SELECT 'empty array';
SELECT arrayElementOrDefault(CAST([], 'Array(UInt64)'), 1, 42);

SELECT 'strings';
SELECT arrayElementOrDefault(['a', 'b'], 2, 'def');
SELECT arrayElementOrDefault(['a', 'b'], 5, 'def');

SELECT 'maps';
SELECT arrayElementOrDefault(map('a', 1, 'b', 2), 'b', -1);
SELECT arrayElementOrDefault(map('a', 1, 'b', 2), 'x', -1);
SELECT arrayElementOrDefault(map(1, 'one', 2, 'two'), 2, 'none');
SELECT arrayElementOrDefault(map(1, 'one', 2, 'two'), 9, 'none');

SELECT 'result type';
SELECT toTypeName(arrayElementOrDefault(CAST([1, 2], 'Array(UInt8)'), 9, 1000));
SELECT arrayElementOrDefault(CAST([1, 2], 'Array(UInt8)'), 9, 1000);
SELECT toTypeName(arrayElementOrDefault([1.5, 2.5], 9, 0));
SELECT arrayElementOrDefault([1.5, 2.5], 1, 0);

-- A present NULL is treated as absent, same as arrayElementOrNull.
SELECT 'nullable element';
SELECT arrayElementOrDefault(CAST([1, NULL, 3], 'Array(Nullable(UInt32))'), 2, 99);
SELECT arrayElementOrDefault(CAST([1, NULL, 3], 'Array(Nullable(UInt32))'), 1, 99);

SELECT 'NULL default';
SELECT arrayElementOrDefault([1, 2, 3], 9, NULL) AS v, toTypeName(v);

SELECT 'non-const index';
SELECT n, arrayElementOrDefault([10, 20, 30], n, -7) AS v FROM (SELECT number - 1 AS n FROM numbers(6)) ORDER BY n;

SELECT 'non-const array and default';
SELECT arr, idx, arrayElementOrDefault(arr, idx, def) AS v
FROM (SELECT [toInt64(number), toInt64(number) + 1] AS arr, toInt64(number % 3) AS idx, toInt64(number * 100) AS def FROM numbers(4))
ORDER BY idx, def;

SELECT 'errors';
SELECT arrayElementOrDefault(5, 1, 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayElementOrDefault([1, 2, 3], 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayElementOrDefault([[1], [2]], 5, [9]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayElementOrDefault([1, 2], 9, 'x'); -- { serverError NO_COMMON_TYPE }
