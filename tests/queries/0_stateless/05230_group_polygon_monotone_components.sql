SET max_threads = 1;

CREATE TEMPORARY TABLE component_holes (id UInt64, p Polygon);
INSERT INTO component_holes
SELECT number,
    [[(0.,0.),(0.,8.),(200.,8.),(200.,0.),(0.,0.)],
     [(1.5 * number + 1,1. + number % 2),(1.5 * number + 3,1. + number % 2),
      (1.5 * number + 3,4. + number % 2),(1.5 * number + 1,4. + number % 2),
      (1.5 * number + 1,1. + number % 2)]]
FROM numbers(65);

SELECT 'reverse', length(g[1]), length(g[1][2]), polygonAreaCartesian(g)
FROM (SELECT groupPolygonIntersection(p) AS g FROM (SELECT * FROM component_holes ORDER BY id DESC));
SELECT 'shuffle', length(g[1]), length(g[1][2]), polygonAreaCartesian(g)
FROM (SELECT groupPolygonIntersection(p) AS g FROM (SELECT * FROM component_holes ORDER BY cityHash64(id), id));
-- First accumulate separate components, then join them through the missing holes.
SELECT 'bridges', length(g[1]), length(g[1][2]), polygonAreaCartesian(g)
FROM (SELECT groupPolygonIntersection(p) AS g FROM (SELECT * FROM component_holes ORDER BY id % 2, id));
SELECT 'both_ends', length(g[1]), length(g[1][2]), polygonAreaCartesian(g)
FROM (SELECT groupPolygonIntersection(p) AS g FROM (SELECT * FROM component_holes ORDER BY abs(toInt64(id) - 32), id));

CREATE TEMPORARY TABLE component_states ENGINE = Memory AS
SELECT intDiv(id, 2) % 3 AS part, groupPolygonIntersectionState(p) AS s
FROM (SELECT * FROM component_holes ORDER BY cityHash64(id), id) GROUP BY part;
SELECT 'direct_merge', length(g[1]), length(g[1][2]), polygonAreaCartesian(g)
FROM (SELECT groupPolygonIntersectionMerge(s) AS g FROM (SELECT * FROM component_states ORDER BY part DESC));
-- Cached finalization must neither consume RHS nor become stale after another merge.
SELECT 'repeat_merge', length(g[1]), polygonAreaCartesian(g)
FROM
(
    SELECT groupPolygonIntersectionMerge(s) AS g FROM
    (
        SELECT s FROM component_states WHERE NOT empty(finalizeAggregation(s))
        UNION ALL
        SELECT s FROM component_states
    )
);

-- For every prefix: area = 1600 - 5*t - c, points = 5 + 4*t + c.
SELECT 'window', count(), min(polygonAreaCartesian(g) + arraySum(arrayMap(p -> arraySum(arrayMap(r -> length(r), p)), g)) = 1605 - t)
FROM
(
    SELECT groupPolygonIntersection(p) OVER (ORDER BY cityHash64(id), id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS g,
        row_number() OVER (ORDER BY cityHash64(id), id) AS t
    FROM component_holes
);

-- A bridge matches the left end but not the right end. Neither old hole may be lost.
SELECT 'second_junction_rejected', length(g[1]), polygonAreaCartesian(g)
FROM
(
    SELECT groupPolygonIntersection(p) AS g FROM
    (
        SELECT [[(0.,0.),(0.,20.),(20.,20.),(20.,0.),(0.,0.)], r]::Polygon AS p
        FROM
        (
            SELECT arrayJoin([
                [(1.,1.),(3.,1.),(3.,4.),(1.,4.),(1.,1.)],
                [(4.,10.),(6.,10.),(6.,13.),(4.,13.),(4.,10.)],
                [(2.5,1.),(4.5,1.),(4.5,4.),(2.5,4.),(2.5,1.)]
            ]) AS r
        )
    )
);

-- One row first adds an eligible hole, then leaves the class with a triangle.
SELECT 'remaining_holes', length(g[1]), polygonAreaCartesian(g)
FROM
(
    SELECT groupPolygonIntersection(p) AS g FROM
    (
        SELECT * FROM
        (
            SELECT * FROM component_holes
            UNION ALL
            SELECT 65, [[(0.,0.),(0.,8.),(200.,8.),(200.,0.),(0.,0.)],
                [(140.,1.),(142.,1.),(142.,3.),(140.,3.),(140.,1.)],
                [(151.,1.),(152.,3.),(153.,1.),(151.,1.)]]::Polygon
        ) ORDER BY id
    )
);
