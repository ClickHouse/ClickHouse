SET max_threads = 1;

CREATE TEMPORARY TABLE rotated_holes (id UInt64, p Polygon);
INSERT INTO rotated_holes
WITH [(0.,0.),(0.,8.),(200.,8.),(200.,0.)] AS exterior,
    arrayRotateLeft(exterior, toInt64(number % 4)) AS shifted,
    arrayPushBack(shifted, shifted[1]) AS closed
SELECT number, [if(number % 2, arrayReverse(closed), closed),
    [(3. * number + 1,1.),(3. * number + 2,1.),(3. * number + 2,2.),
     (3. * number + 1,2.),(3. * number + 1,1.)]]
FROM numbers(65);

SELECT 'rotated', length(g), length(g[1]), polygonAreaCartesian(g)
FROM (SELECT groupPolygonIntersection(p) AS g FROM rotated_holes);
SELECT 'merge', length(g), length(g[1]), polygonAreaCartesian(g)
FROM
(
    SELECT groupPolygonIntersectionMerge(s) AS g FROM
    (
        SELECT groupPolygonIntersectionState(p) AS s
        FROM rotated_holes GROUP BY intDiv(id, 8)
    )
);
SELECT 'window', count(), sum(length(g[1])), sum(polygonAreaCartesian(g))
FROM
(
    SELECT groupPolygonIntersection(p) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS g
    FROM rotated_holes
);

-- Preserve repeated vertices when comparing cycles.
SELECT 'duplicates', length(g[1]), polygonAreaCartesian(g)
FROM
(
    SELECT groupPolygonIntersection(arrayConcat([arrayPushFront(p[1], p[1][1])], arraySlice(p, 2))::Polygon) AS g
    FROM rotated_holes
);

-- Interleaved component boxes defeat separation of the global envelopes.
CREATE TEMPORARY TABLE interleaved_union (id UInt64, p Polygon);
INSERT INTO interleaved_union SELECT number,
    [[(3. * number,0.),(3. * number,1.),(3. * number + 1,1.),(3. * number + 1,0.),(3. * number,0.)]]
FROM numbers(128);
SELECT 'interleaved', length(g), polygonAreaCartesian(g)
FROM
(
    SELECT groupPolygonUnionMerge(s) AS g FROM
    (
        SELECT groupPolygonUnionState(p) AS s FROM interleaved_union GROUP BY id % 2
    )
);

-- Phase 5 may skip boundary-only boxes only after invalid contacts are rejected.
SELECT groupPolygonUnion(mp) FROM
(
    SELECT [[[(0.,0.),(0.,1.),(1.,1.),(1.,0.),(0.,0.)]],
            [[(1.,0.),(1.,1.),(2.,1.),(2.,0.),(1.,0.)]]]::MultiPolygon AS mp
); -- { serverError BAD_ARGUMENTS }
SELECT 'corners', length(g), polygonAreaCartesian(g)
FROM
(
    SELECT groupPolygonUnion(mp) AS g FROM
    (
        SELECT [[[(0.,0.),(0.,1.),(1.,1.),(1.,0.),(0.,0.)]],
                [[(1.,1.),(1.,2.),(2.,2.),(2.,1.),(1.,1.)]]]::MultiPolygon AS mp
    )
);

-- Boost accepts an approximately closed ring; its last vertex must not be discarded by cycle comparison.
SELECT 'near_closure', g[1][1][1] = g[1][1][-1],
    arrayExists(point -> point = (1.0000000000000002, 1.), g[1][1])
FROM
(
    SELECT groupPolygonIntersection(p) AS g FROM
    (
        SELECT arrayJoin([
            [[(1.,1.),(1.,4.),(4.,4.),(4.,1.),(1.0000000000000002,1.)]],
            [[(1.,4.),(4.,4.),(4.,1.),(1.,1.),(1.,4.)]]
        ])::Polygon AS p
    )
);
