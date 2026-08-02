-- `length(arrayFilter(f, arr, ...))` is rewritten to `arrayCount(f, arr, ...)`, which counts the
-- matching elements instead of building an array of them only to take its size.

SELECT '-- the rewrite fires';
SELECT
    countIf(explain LIKE '%function_name: arrayCount%') AS array_count,
    countIf(explain LIKE '%function_name: arrayFilter%') AS array_filter
FROM (EXPLAIN QUERY TREE SELECT length(arrayFilter(x -> (x > 1), materialize([1, 2, 3]))));

SELECT '-- and not when it is switched off';
SELECT
    countIf(explain LIKE '%function_name: arrayCount%') AS array_count,
    countIf(explain LIKE '%function_name: arrayFilter%') AS array_filter
FROM (EXPLAIN QUERY TREE SELECT length(arrayFilter(x -> (x > 1), materialize([1, 2, 3]))) SETTINGS optimize_rewrite_array_filter_length_to_array_count = 0);

SELECT '-- the result type is not changed';
SELECT toTypeName(length(arrayFilter(x -> (x > 1), materialize([1, 2, 3]))));
SELECT toTypeName(length(arrayFilter(x -> (x > 1), materialize([1, 2, 3])))) SETTINGS optimize_rewrite_array_filter_length_to_array_count = 0;

SELECT '-- the same results with the rewrite on';
SELECT groupArray(length(arrayFilter(x -> (x > 2), arr)))
FROM (SELECT arrayMap(x -> (x % 5), range(number)) AS arr FROM numbers(8))
SETTINGS optimize_rewrite_array_filter_length_to_array_count = 1;
SELECT '-- and off';
SELECT groupArray(length(arrayFilter(x -> (x > 2), arr)))
FROM (SELECT arrayMap(x -> (x % 5), range(number)) AS arr FROM numbers(8))
SETTINGS optimize_rewrite_array_filter_length_to_array_count = 0;

SELECT '-- lambdas returning NULL, several arrays, a plain expression body';
SELECT length(arrayFilter(x -> NULL, [1, 2]));
SELECT length(arrayFilter(x -> (x > 1) ? NULL : 1, [1, 2, 3]));
SELECT length(arrayFilter(x -> toNullable(x % 2), materialize([1, 2, 3])));
SELECT length(arrayFilter((x, y) -> (x > y), materialize([1, 2, 3]), [0, 5, 1]));
SELECT length(arrayFilter(x -> x, materialize([0, 1, 2])));
SELECT length(arrayFilter(x -> 2, materialize([1, 2, 3])));
SELECT length(arrayFilter(x -> (x > 'a'), materialize(['a', 'b', 'c'])));
SELECT length(arrayFilter(x -> isNotNull(x), materialize([1, NULL, 3])));
SELECT length(arrayFilter(x -> (x > 1), CAST([], 'Array(UInt8)')));

SELECT '-- an alias on the outer call';
SELECT length(arrayFilter(x -> (x > 1), materialize([1, 2, 3]))) AS c, c, c + 1;

SELECT '-- the filtered array is also used on its own';
WITH arrayFilter(x -> (x > 1), materialize([1, 2, 3])) AS f
SELECT length(f), f;

SELECT '-- length of something that is not arrayFilter is left alone';
SELECT length(materialize('abc')), length(materialize([1, 2])), length(materialize(map(1, 2)));
SELECT length(arrayMap(x -> (x + 1), materialize([1, 2, 3])));
