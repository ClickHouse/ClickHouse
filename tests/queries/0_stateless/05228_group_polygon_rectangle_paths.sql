SET max_threads = 1;

CREATE TEMPORARY TABLE rectangle_pairs (name String, p Polygon);
INSERT INTO rectangle_pairs VALUES
('aligned', [[(0.,0.),(0.,2.),(2.,2.),(2.,0.),(0.,0.)]]),
('aligned', [[(1.,0.),(1.,2.),(3.,2.),(3.,0.),(1.,0.)]]),
('vertical', [[(0.,0.),(0.,2.),(2.,2.),(2.,0.),(0.,0.)]]),
('vertical', [[(0.,1.),(0.,3.),(2.,3.),(2.,1.),(0.,1.)]]),
('nested', [[(0.,0.),(0.,2.),(2.,2.),(2.,0.),(0.,0.)]]),
('nested', [[(0.5,0.5),(0.5,1.5),(1.5,1.5),(1.5,0.5),(0.5,0.5)]]),
('diagonal', [[(0.,0.),(0.,2.),(2.,2.),(2.,0.),(0.,0.)]]),
('diagonal', [[(1.,1.),(1.,3.),(3.,3.),(3.,1.),(1.,1.)]]),
('edge', [[(0.,0.),(0.,2.),(2.,2.),(2.,0.),(0.,0.)]]),
('edge', [[(2.,0.),(2.,2.),(4.,2.),(4.,0.),(2.,0.)]]),
('corner', [[(0.,0.),(0.,2.),(2.,2.),(2.,0.),(0.,0.)]]),
('corner', [[(2.,2.),(2.,4.),(4.,4.),(4.,2.),(2.,2.)]]),
('gap', [[(0.,0.),(0.,2.),(2.,2.),(2.,0.),(0.,0.)]]),
('gap', [[(3.,0.),(3.,2.),(5.,2.),(5.,0.),(3.,0.)]]);
SELECT name, length(g), polygonAreaCartesian(g)
FROM (SELECT name, groupPolygonUnion(p) AS g FROM rectangle_pairs GROUP BY name)
ORDER BY name;

-- A component certificate must reject shared edges, even for exact rectangles.
SELECT groupPolygonUnion(mp) FROM
(
    SELECT [[[(0.,0.),(0.,1.),(1.,1.),(1.,0.),(0.,0.)]],
            [[(1.,0.),(1.,1.),(2.,1.),(2.,0.),(1.,0.)]]]::MultiPolygon AS mp
); -- { serverError BAD_ARGUMENTS }

SELECT 'corner_chain', length(g), polygonAreaCartesian(g)
FROM
(
    SELECT groupPolygonUnion(mp) AS g FROM
    (
        SELECT arrayMap(i -> [[(toFloat64(i),toFloat64(i)),(i + 1.,toFloat64(i)),
            (i + 1.,i + 1.),(toFloat64(i),i + 1.),(toFloat64(i),toFloat64(i))]], range(128))::MultiPolygon AS mp
    )
);

SELECT 'crossbars', length(g), polygonAreaCartesian(g),
    arrayAll(p -> length(p) = 1 AND length(p[1]) = 5 AND polygonAreaCartesian(p::Polygon) = 1, g)
FROM
(
    SELECT groupPolygonIntersection(mp) AS g FROM
    (
        SELECT if(number = 0,
            arrayMap(i -> [[(0.,3. * i),(0.,3. * i + 1),(12.,3. * i + 1),(12.,3. * i),(0.,3. * i)]], range(4)),
            arrayMap(i -> [[(3. * i,0.),(3. * i,12.),(3. * i + 1,12.),(3. * i + 1,0.),(3. * i,0.)]], range(4)))::MultiPolygon AS mp
        FROM numbers(2)
    )
);

-- Preserve the general overlay's treatment of numerically collapsed intersections.
SELECT 'thin_overlap', empty(groupPolygonIntersection(mp)) FROM
(
    SELECT arrayJoin([
        [[[(1.,1.),(1.,4.),(3.,4.),(3.,1.),(1.,1.)]],
         [[(6.,1.),(6.,4.),(8.,4.),(8.,1.),(6.,1.)]]],
        [[[(2.9999999999999996,1.),(2.9999999999999996,4.),(5.,4.),(5.,1.),(2.9999999999999996,1.)]],
         [[(9.,1.),(9.,4.),(10.,4.),(10.,1.),(9.,1.)]]]
    ])::MultiPolygon AS mp
);
