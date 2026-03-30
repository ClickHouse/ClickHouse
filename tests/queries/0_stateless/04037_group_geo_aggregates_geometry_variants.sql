-- Test Geometry variant mixed scenarios

-- 1. groupConvexHull on mixed Geometry types (Point + Polygon outer ring)
SELECT 'convex_hull_mixed_geometry';
SELECT round(polygonAreaCartesian(groupConvexHull(g)), 2) FROM (
    SELECT arrayJoin([
        readWKT('POINT (0 0)'),
        readWKT('POINT (10 0)'),
        readWKT('POINT (10 10)'),
        readWKT('POINT (0 10)')
    ]) AS g
);

-- 2. groupPolygonUnion on Geometry polygons
SELECT 'union_geometry_polygons';
SELECT round(polygonAreaCartesian(groupPolygonUnion(g)), 2) FROM (
    SELECT arrayJoin([
        readWKT('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))'),
        readWKT('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))')
    ]) AS g
);

-- 3. groupPolygonIntersection on Geometry polygons
SELECT 'intersect_geometry_polygons';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(g)), 2) FROM (
    SELECT arrayJoin([
        readWKT('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))'),
        readWKT('POLYGON ((2 2, 2 6, 6 6, 6 2, 2 2))')
    ]) AS g
);

-- 4. groupConvexHull with Geometry containing a polygon (should use outer ring points)
SELECT 'convex_hull_geometry_polygon';
SELECT round(polygonAreaCartesian(groupConvexHull(g)), 2) FROM (
    SELECT readWKT('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))') AS g
);

-- 5. groupPolygonUnion with single Geometry polygon
SELECT 'union_single_geometry';
SELECT round(polygonAreaCartesian(groupPolygonUnion(g)), 2) FROM (
    SELECT readWKT('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))') AS g
);

-- 6. groupPolygonIntersection with single Geometry polygon
SELECT 'intersect_single_geometry';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(g)), 2) FROM (
    SELECT readWKT('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))') AS g
);

-- 7. Union of three Geometry polygons with partial overlaps
SELECT 'union_three_geometry';
SELECT round(polygonAreaCartesian(groupPolygonUnion(g)), 2) FROM (
    SELECT arrayJoin([
        readWKT('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'),
        readWKT('POLYGON ((1 0, 1 2, 3 2, 3 0, 1 0))'),
        readWKT('POLYGON ((2 0, 2 2, 4 2, 4 0, 2 0))')
    ]) AS g
);

-- 8. Intersect of three Geometry polygons
SELECT 'intersect_three_geometry';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(g)), 2) FROM (
    SELECT arrayJoin([
        readWKT('POLYGON ((0 0, 0 6, 6 6, 6 0, 0 0))'),
        readWKT('POLYGON ((1 1, 1 5, 5 5, 5 1, 1 1))'),
        readWKT('POLYGON ((2 2, 2 4, 4 4, 4 2, 2 2))')
    ]) AS g
);

-- 9. groupConvexHull with all five Geometry runtime types in one group
SELECT 'convex_hull_mixed_geometry_runtime_types';
SELECT
    round(polygonAreaCartesian(groupConvexHull(g)), 2) AS area,
    round(polygonPerimeterCartesian(groupConvexHull(g)), 2) AS perim
FROM (
    SELECT arrayJoin([
        readWKT('POINT (0 0)'),
        readWKT('LINESTRING (20 0, 15 5)'),
        readWKT('MULTILINESTRING ((10 0, 5 10), (15 15, 20 20))'),
        readWKT('POLYGON ((0 15, 0 20, 5 20, 5 15, 0 15))'),
        readWKT('MULTIPOLYGON (((10 10, 10 12, 12 12, 12 10, 10 10)))')
    ]) AS g
);

-- 10. groupPolygonUnion mixing Polygon and MultiPolygon via Geometry
SELECT 'union_mixed_polygon_multipolygon';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(actual, expected)), 4) < 0.001 AS equiv
FROM (
    SELECT
        (SELECT groupPolygonUnion(g) FROM (
            SELECT arrayJoin([
                readWKT('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))'),
                readWKT('MULTIPOLYGON (((2 2, 2 5, 5 5, 5 2, 2 2)))')
            ]) AS g
        )) AS actual,
        (SELECT groupPolygonUnion(p) FROM (
            SELECT arrayJoin([
                readWKTPolygon('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))'),
                readWKTPolygon('POLYGON ((2 2, 2 5, 5 5, 5 2, 2 2))')
            ]) AS p
        )) AS expected
);

-- 11. groupPolygonIntersection mixing Polygon and MultiPolygon via Geometry
SELECT 'intersect_mixed_polygon_multipolygon';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(actual, expected)), 4) < 0.001 AS equiv
FROM (
    SELECT
        (SELECT groupPolygonIntersection(g) FROM (
            SELECT arrayJoin([
                readWKT('POLYGON ((0 0, 0 6, 6 6, 6 0, 0 0))'),
                readWKT('MULTIPOLYGON (((2 2, 2 8, 8 8, 8 2, 2 2)))')
            ]) AS g
        )) AS actual,
        (SELECT groupPolygonIntersection(p) FROM (
            SELECT arrayJoin([
                readWKTPolygon('POLYGON ((0 0, 0 6, 6 6, 6 0, 0 0))'),
                readWKTPolygon('POLYGON ((2 2, 2 8, 8 8, 8 2, 2 2))')
            ]) AS p
        )) AS expected
);
