SELECT
    arrayReduceInRanges(
        'groupArray',
        [(1, 3), (2, 3), (3, 3)],
        ['a', 'b', 'c', 'd', 'e']
    );

SELECT
    arrayReduceInRanges(
        'sum',
        [
            (-6, 0), (-4, 0), (-2, 0), (0, 0), (2, 0), (4, 0),
            (-6, 1), (-4, 1), (-2, 1), (0, 1), (2, 1), (4, 1),
            (-6, 2), (-4, 2), (-2, 2), (0, 2), (2, 2), (4, 2),
            (-6, 3), (-4, 3), (-2, 3), (0, 3), (2, 3), (4, 3)
        ],
        [100, 200, 300, 400]
    );

WITH
    arrayMap(x -> x + 1, range(50)) as data
SELECT
    arrayReduceInRanges('groupArray', [(a, c), (b, d)], data) =
        [arraySlice(data, a, c), arraySlice(data, b, d)]
FROM (
    SELECT
        cityHash64(number + 100) % 40 as a,
        cityHash64(number + 200) % 60 as b,
        cityHash64(number + 300) % 20 as c,
        cityHash64(number + 400) % 30 as d
    FROM numbers(20)
);

-- Arrays long enough for pre-aggregation, spread over several rows and over several argument arrays.
-- The block layout is pinned so all 4 rows reach the function in one block, i.e. with a non-zero row offset.
SET max_block_size = 4, max_threads = 1;

WITH arrayMap(x -> x + number * 1000, range(200)) AS arr
SELECT number, arrayReduceInRanges('sum', [(1, 200), (33, 100), (129, 64)], arr)
FROM numbers(4) ORDER BY number;

WITH arrayMap(x -> x + number * 1000, range(200)) AS arr, arrayMap(x -> toUInt8(x % 2), range(200)) AS cond
SELECT number, arrayReduceInRanges('sumIf', [(1, 200), (33, 100)], arr, cond)
FROM numbers(4) ORDER BY number;

WITH arrayMap(x -> x + number * 1000, range(200)) AS arr
SELECT number, arrayReduceInRanges('groupArray', [(1, 200), (33, 100)], arr) = [arraySlice(arr, 1, 200), arraySlice(arr, 33, 100)]
FROM numbers(4) ORDER BY number;

WITH arrayMap(x -> x + number * 1000, range(200)) AS arr, arrayMap(x -> toUInt8(x % 2), range(200)) AS cond
SELECT number, arrayReduceInRanges('sumIf', [(1, 200)], arrayMap(x -> x + NULL, arr), cond)
FROM numbers(4) ORDER BY number;

SELECT arrayReduceInRanges('sumIf', [(1, 3)], [1, 2, 3], [1, 1]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
