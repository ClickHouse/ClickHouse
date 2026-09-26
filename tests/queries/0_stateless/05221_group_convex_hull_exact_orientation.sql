SET max_threads = 1;

-- Check vertices, not area: area itself underflows for the smallest triangles.
CREATE TEMPORARY TABLE hull_exact_points (exponent Int16, p Point);
INSERT INTO hull_exact_points
SELECT exponent, (unit.1 * exp2(exponent), unit.2 * exp2(exponent))::Point
FROM (SELECT arrayJoin([-1074, -1073, -1022, -1000, -665, -512, -300, -128, -33, 0, 128, 512, 665, 1000, 1023]) AS exponent)
ARRAY JOIN [(0., 0.), (1., 0.), (0., 1.)] AS unit;

SELECT 'direct', count(), countIf(vertices = [(0., 0.), (0., 1.), (1., 0.)])
FROM
(
    SELECT exponent, arraySort(arrayDistinct(arrayMap(p -> (p.1 / exp2(exponent), p.2 / exp2(exponent)), groupConvexHull(p)))) AS vertices
    FROM hull_exact_points GROUP BY exponent
);

SELECT 'reverse', count(), countIf(vertices = [(0., 0.), (0., 1.), (1., 0.)])
FROM
(
    SELECT exponent, arraySort(arrayDistinct(arrayMap(p -> (p.1 / exp2(exponent), p.2 / exp2(exponent)), groupConvexHull(p)))) AS vertices
    FROM (SELECT * FROM hull_exact_points ORDER BY exponent DESC, p DESC) GROUP BY exponent
);

CREATE TEMPORARY TABLE hull_exact_states (exponent Int16, s AggregateFunction(groupConvexHull, Point));
INSERT INTO hull_exact_states SELECT exponent, groupConvexHullState(p) FROM hull_exact_points GROUP BY exponent;

SELECT 'binary_roundtrip', count(), countIf(vertices = [(0., 0.), (0., 1.), (1., 0.)])
FROM
(
    SELECT exponent, arraySort(arrayDistinct(arrayMap(p -> (p.1 / exp2(exponent), p.2 / exp2(exponent)),
        finalizeAggregation(CAST(unhex(hex(s)) AS AggregateFunction(groupConvexHull, Point)))))) AS vertices
    FROM hull_exact_states
);

TRUNCATE TABLE hull_exact_states;
-- 10,002 points force compression. Every exponent exercises the same triangle.
INSERT INTO hull_exact_states
SELECT exponent, groupConvexHullState(p)
FROM hull_exact_points ARRAY JOIN range(3334) AS repetition GROUP BY exponent;

SELECT 'compression_roundtrip', count(), countIf(vertices = [(0., 0.), (0., 1.), (1., 0.)])
FROM
(
    SELECT exponent, arraySort(arrayDistinct(arrayMap(p -> (p.1 / exp2(exponent), p.2 / exp2(exponent)),
        finalizeAggregation(CAST(unhex(hex(s)) AS AggregateFunction(groupConvexHull, Point)))))) AS vertices
    FROM hull_exact_states
);

SELECT 'merge', count(), countIf(vertices = [(0., 0.), (0., 1.), (1., 0.)])
FROM
(
    SELECT exponent, arraySort(arrayDistinct(arrayMap(p -> (p.1 / exp2(exponent), p.2 / exp2(exponent)), groupConvexHullMerge(s)))) AS vertices
    FROM
    (
        SELECT exponent, CAST(unhex(hex(groupConvexHullState(p))) AS AggregateFunction(groupConvexHull, Point)) AS s
        FROM hull_exact_points GROUP BY exponent, p
    ) GROUP BY exponent
);

-- Mixed scales, near-collinearity, and overflowing differences of finite coordinates.
SELECT 'mixed_scales', id, arraySort(arrayDistinct(groupConvexHull(p))) = arraySort(groupUniqArray(p))
FROM
(
    SELECT 1 AS id, arrayJoin([(0., 0.), (1e308, 1e308), (1e-308, 0.)])::Point AS p
    UNION ALL
    SELECT 2 AS id, arrayJoin([(-1e308, -1e308), (1e308, 1e308), (0., 1e-308)])::Point AS p
    UNION ALL
    SELECT 3 AS id, arrayJoin([(0., 0.), (1., 1.), (1.0000000000000002, 1.)])::Point AS p
    UNION ALL
    SELECT 4 AS id, arrayJoin([(0., 0.), (1e308, 1e-308), (1e308, 0.)])::Point AS p
) GROUP BY id ORDER BY id;
