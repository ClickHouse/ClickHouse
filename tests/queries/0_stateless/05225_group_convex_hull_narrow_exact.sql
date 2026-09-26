SET max_threads = 1;

-- Exercise cancellation on both sides of the native-integer exponent-spread bound.
CREATE TEMPORARY TABLE narrow_exact_points (id Tuple(Int16, UInt8, Int8), p Point);
INSERT INTO narrow_exact_points
SELECT (exponent, spread, direction), (unit.1 * exp2(exponent) * direction, unit.2 * exp2(exponent))
FROM
(
    SELECT arrayJoin([-1020, -1000, 0, 500, 1000])::Int16 AS exponent,
        arrayJoin([8, 9, 10])::UInt8 AS spread,
        arrayJoin([-1, 1])::Int8 AS direction
)
ARRAY JOIN [(0., 0.), (1., 1.), (exp2(spread), exp2(spread) * (1 + exp2(-52)))] AS unit;

SELECT 'direct', count(), countIf(actual = expected AND length(actual) = 3)
FROM
(
    SELECT id, arraySort(arrayDistinct(groupConvexHull(p))) AS actual, arraySort(groupUniqArray(p)) AS expected
    FROM narrow_exact_points GROUP BY id
);

SELECT 'compression', count(), countIf(actual = expected AND length(actual) = 3)
FROM
(
    SELECT id, arraySort(arrayDistinct(groupConvexHull(p))) AS actual, arraySort(groupUniqArray(p)) AS expected
    FROM narrow_exact_points ARRAY JOIN range(3334) AS repetition GROUP BY id
);

SELECT 'binary_roundtrip', count(), countIf(actual = expected AND length(actual) = 3)
FROM
(
    SELECT id,
        arraySort(arrayDistinct(finalizeAggregation(CAST(unhex(hex(s)) AS AggregateFunction(groupConvexHull, Point))))) AS actual,
        expected
    FROM (SELECT id, groupConvexHullState(p) AS s, arraySort(groupUniqArray(p)) AS expected FROM narrow_exact_points GROUP BY id)
);

SELECT 'merge', count(), countIf(actual = expected AND length(actual) = 3)
FROM
(
    SELECT id, arraySort(arrayDistinct(groupConvexHullMerge(s))) AS actual, arraySort(groupUniqArray(p)) AS expected
    FROM
    (
        SELECT id, p, CAST(unhex(hex(groupConvexHullState(p))) AS AggregateFunction(groupConvexHull, Point)) AS s
        FROM narrow_exact_points GROUP BY id, p
    ) GROUP BY id
);
