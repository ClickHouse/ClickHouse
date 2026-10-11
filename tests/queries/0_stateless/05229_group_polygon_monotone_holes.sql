SET max_threads = 1;

CREATE TEMPORARY TABLE monotone_holes (id UInt64, p Polygon);
INSERT INTO monotone_holes
WITH [(0.,0.),(0.,8.),(200.,8.),(200.,0.)] AS exterior,
    arrayRotateLeft(exterior, toInt64(number % 4)) AS shifted
SELECT number, [arrayPushBack(shifted, shifted[1]),
    [(1.5 * number + 1,1. + number % 2),(1.5 * number + 3,1. + number % 2),
     (1.5 * number + 3,4. + number % 2),(1.5 * number + 1,4. + number % 2),
     (1.5 * number + 1,1. + number % 2)]]
FROM numbers(65);

SELECT 'monotone', length(g), length(g[1]), length(g[1][2]), polygonAreaCartesian(g)
FROM (SELECT groupPolygonIntersection(p) AS g FROM (SELECT * FROM monotone_holes ORDER BY id));

SELECT 'merge_after_finalize', length(g), polygonAreaCartesian(g)
FROM
(
    SELECT groupPolygonIntersectionMerge(s) AS g FROM
    (
        SELECT s, finalizeAggregation(s) AS partial FROM
        (
            SELECT groupPolygonIntersectionState(p) AS s
            FROM (SELECT * FROM monotone_holes ORDER BY id)
            GROUP BY intDiv(id, 8)
        )
        WHERE NOT empty(partial)
    )
);

-- Each prefix is materialized before the next row updates the retained boundary.
SELECT 'window', count(), sum(polygonAreaCartesian(g)),
    sum(arraySum(arrayMap(p -> arraySum(arrayMap(r -> length(r), p)), g)))
FROM
(
    SELECT groupPolygonIntersection(p) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS g
    FROM monotone_holes
);

-- Leave the monotone path with a new, disjoint hole inside the common exterior.
SELECT 'extra_hole', length(g[1]), polygonAreaCartesian(g)
FROM
(
    SELECT groupPolygonIntersection(p) AS g FROM
    (
        SELECT * FROM
        (
        SELECT * FROM monotone_holes
        UNION ALL
        SELECT 65, [[(0.,0.),(0.,8.),(200.,8.),(200.,0.),(0.,0.)],
                    [(150.,1.),(152.,1.),(152.,3.),(150.,3.),(150.,1.)]]::Polygon
        ) ORDER BY id
    )
);

-- Leaving the optimized state must still expose an empty result before an invalid row.
SELECT 'empty_then_nan', empty(g)
FROM
(
    SELECT groupPolygonIntersection(p) AS g FROM
    (
        SELECT * FROM
        (
        SELECT * FROM monotone_holes
        UNION ALL
        SELECT 65, [[(-5.,-5.),(-5.,-4.),(-4.,-4.),(-4.,-5.),(-5.,-5.)]]::Polygon
        UNION ALL
        SELECT 66, [[(nan,0.),(0.,1.),(1.,0.),(nan,0.)]]::Polygon
        ) ORDER BY id
    )
);

-- Adjacent slabs may meet at exactly the same coordinate; junction edges cancel.
SELECT 'equal_slabs', length(g[1][2]), polygonAreaCartesian(g)
FROM
(
    SELECT groupPolygonIntersection(p) AS g FROM
    (
        SELECT [[(0.,0.),(0.,8.),(200.,8.),(200.,0.),(0.,0.)],
            [(number + 1.,1. + number % 2),(number + 3.,1. + number % 2),
             (number + 3.,4. + number % 2),(number + 1.,4. + number % 2),
             (number + 1.,1. + number % 2)]]::Polygon AS p
        FROM numbers(65)
    )
);
