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
