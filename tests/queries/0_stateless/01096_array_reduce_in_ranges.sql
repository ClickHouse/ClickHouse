DROP TABLE IF EXISTS t_array_reduce_in_ranges;
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

DROP TABLE IF EXISTS t_array_reduce_in_ranges;

CREATE TABLE t_array_reduce_in_ranges (id UInt64, arr Array(UInt64), cond Array(UInt8)) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_array_reduce_in_ranges
SELECT number, arrayMap(x -> x + number * 1000, range(200)), arrayMap(x -> toUInt8(x % 2), range(200))
FROM numbers(4);

SELECT id, arrayReduceInRanges('sum', [(1, 200), (33, 100), (129, 64)], arr)
FROM t_array_reduce_in_ranges ORDER BY id;

SELECT id, arrayReduceInRanges('sumIf', [(1, 200), (33, 100)], arr, cond)
FROM t_array_reduce_in_ranges ORDER BY id;

SELECT id, arrayReduceInRanges('groupArray', [(1, 200), (33, 100)], arr) = [arraySlice(arr, 1, 200), arraySlice(arr, 33, 100)]
FROM t_array_reduce_in_ranges ORDER BY id;

SELECT id, arrayReduceInRanges('sumIf', [(1, 200)], arrayMap(x -> x + NULL, arr), cond)
FROM t_array_reduce_in_ranges ORDER BY id;

SELECT arrayReduceInRanges('sumIf', [(1, 3)], [1, 2, 3], [1, 1]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE t_array_reduce_in_ranges;
