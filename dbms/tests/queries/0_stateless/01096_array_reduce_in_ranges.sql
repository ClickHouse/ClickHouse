SELECT
    arrayReduceInRanges(
        'groupArray',
        [1, 2, 3],
        [3, 3, 3],
        ['a', 'b', 'c', 'd', 'e']
    );

SELECT
    arrayReduceInRanges(
        'sum',
        [0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5],
        [0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3],
        [100, 200, 300, 400]
    );

WITH
    arrayMap(x -> x + 1, range(50)) as data
SELECT
    arrayReduceInRanges('groupArray', [a, b], [c, d], data) =
        [arraySlice(data, a, c), arraySlice(data, b, d)]
FROM (
    SELECT
        cityHash64(number + 100) % 40 as a,
        cityHash64(number + 200) % 60 as b,
        cityHash64(number + 300) % 20 as c,
        cityHash64(number + 400) % 30 as d
    FROM numbers(20)
);
