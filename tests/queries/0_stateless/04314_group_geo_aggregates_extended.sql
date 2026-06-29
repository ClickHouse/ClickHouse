-- Extended coverage for geo aggregate functions

-- groupConvexHull accepts LineString
SELECT 'linestring_input';
SELECT wkt(groupConvexHull(ls)) FROM
(
    SELECT readWKTLineString('LINESTRING (0 0, 1 0, 0 1)') AS ls
);

-- groupConvexHull accepts Geometry values with MultiLineString
SELECT 'multilinestring_geometry_input';
SELECT wkt(groupConvexHull(g)) FROM
(
    SELECT arrayJoin([
        readWKT('MULTILINESTRING ((0 0, 1 0), (0 1, 1 1))'),
        readWKT('POINT (2 0)')
    ]) AS g
);

-- groupPolygonUnion accepts polygonal Geometry values
SELECT 'polygon_union_geometry_input';
SELECT round(polygonAreaCartesian(groupPolygonUnion(g)), 2) FROM
(
    SELECT arrayJoin([
        readWKT('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'),
        readWKT('POLYGON ((1 0, 1 1, 2 1, 2 0, 1 0))')
    ]) AS g
);

-- groupPolygonIntersection accepts polygonal Geometry values
SELECT 'polygon_intersect_geometry_input';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(g)), 2) FROM
(
    SELECT arrayJoin([
        readWKT('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'),
        readWKT('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))')
    ]) AS g
);

-- groupConvexHull compression path (> 10000 points)
SELECT 'convex_hull_compression_threshold';
SELECT abs(polygonAreaCartesian(groupConvexHull(pt)) - 4) < 0.001 FROM
(
    SELECT multiIf(
        number % 4 = 0, (0., 0.)::Point,
        number % 4 = 1, (2., 0.)::Point,
        number % 4 = 2, (2., 2.)::Point,
        (0., 2.)::Point) AS pt
    FROM numbers(10005)
);

-- groupPolygonUnion reduction path (> 16 chunks)
SELECT 'polygon_union_reduction_threshold';
SELECT round(polygonAreaCartesian(groupPolygonUnion(p)), 2) FROM
(
    SELECT readWKTPolygon(concat(
        'POLYGON ((',
        toString(number * 2), ' 0, ',
        toString(number * 2), ' 1, ',
        toString(number * 2 + 1), ' 1, ',
        toString(number * 2 + 1), ' 0, ',
        toString(number * 2), ' 0))')) AS p
    FROM numbers(17)
);

-- groupPolygonIntersection reduction path (> 8 chunks)
SELECT 'polygon_intersect_reduction_threshold';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(p)), 2) FROM
(
    SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p
    FROM numbers(9)
);
