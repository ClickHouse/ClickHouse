SET max_threads = 1;

CREATE TEMPORARY TABLE union_components (id UInt64, p Polygon);
INSERT INTO union_components SELECT number,
    [[(3. * number, 0.), (3. * number, 1.), (3. * number + 1, 1.),
      (3. * number + 1, 0.), (3. * number, 0.)]]
FROM numbers(65);

SELECT 'direct', length(g), polygonAreaCartesian(g)
FROM (SELECT groupPolygonUnion(p) AS g FROM union_components);
SELECT 'reverse', length(g), polygonAreaCartesian(g)
FROM (SELECT groupPolygonUnion(p) AS g FROM (SELECT * FROM union_components ORDER BY id DESC));
SELECT 'binary_merge', length(g), polygonAreaCartesian(g)
FROM
(
    SELECT groupPolygonUnionMerge(s) AS g FROM
    (
        SELECT CAST(unhex(hex(groupPolygonUnionState(p))) AS AggregateFunction(groupPolygonUnion, Polygon)) AS s
        FROM union_components GROUP BY intDiv(id, 8)
    )
);

-- Finalization followed by another input must keep the accumulated union correct.
SELECT 'window', count(), sum(length(g)), sum(polygonAreaCartesian(g))
FROM
(
    SELECT groupPolygonUnion(p) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS g
    FROM union_components
);

-- A strict gap allows concatenation; contacts and overlapping boxes still require overlay.
SELECT 'edge', length(g), polygonAreaCartesian(g)
FROM
(
    SELECT groupPolygonUnion(p) AS g FROM
    (
        SELECT [[(x, 0.), (x, 1.), (x + 1, 1.), (x + 1, 0.), (x, 0.)]]::Polygon AS p
        FROM (SELECT toFloat64(number) AS x FROM numbers(65))
    )
);

-- Invalid topology must not become accepted when it is far from another component.
SELECT groupPolygonUnion(p) FROM
(
    SELECT arrayJoin([
        [[(0.,0.),(0.,1.),(1.,1.),(1.,0.),(0.,0.)]],
        [[(3.,0.),(3.,1.),(4.,1.),(4.,0.),(3.,0.)]],
        [[(100.,0.),(102.,2.),(100.,2.),(102.,0.),(100.,0.)]]
    ])::Polygon AS p
); -- { serverError BAD_ARGUMENTS }
