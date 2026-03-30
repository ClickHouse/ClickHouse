-- Test order invariance for geo aggregate functions

-- 1. groupConvexHull: forward vs reverse order
SELECT 'convex_hull_forward_vs_reverse';
SELECT
    abs(polygonAreaCartesian(h1) - polygonAreaCartesian(h2)) < 0.001 AS area_eq,
    abs(polygonPerimeterCartesian(h1) - polygonPerimeterCartesian(h2)) < 0.001 AS perim_eq
FROM (
    SELECT
        (SELECT groupConvexHull(pt) FROM (
            SELECT arrayJoin([
                readWKTPoint('POINT (0 0)'),
                readWKTPoint('POINT (10 0)'),
                readWKTPoint('POINT (10 10)'),
                readWKTPoint('POINT (0 10)'),
                readWKTPoint('POINT (5 5)'),
                readWKTPoint('POINT (5 0)')
            ]) AS pt
        )) AS h1,
        (SELECT groupConvexHull(pt) FROM (
            SELECT arrayJoin([
                readWKTPoint('POINT (5 0)'),
                readWKTPoint('POINT (5 5)'),
                readWKTPoint('POINT (0 10)'),
                readWKTPoint('POINT (10 10)'),
                readWKTPoint('POINT (10 0)'),
                readWKTPoint('POINT (0 0)')
            ]) AS pt
        )) AS h2
);

-- 2. groupPolygonUnion: forward vs reverse (3 overlapping polygons)
SELECT 'polygon_union_forward_vs_reverse';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(u1, u2)), 4) < 0.001 AS equiv
FROM (
    SELECT
        (SELECT groupPolygonUnion(p) FROM (
            SELECT arrayJoin([
                readWKTPolygon('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))'),
                readWKTPolygon('POLYGON ((2 0, 2 4, 6 4, 6 0, 2 0))'),
                readWKTPolygon('POLYGON ((4 0, 4 4, 8 4, 8 0, 4 0))')
            ]) AS p
        )) AS u1,
        (SELECT groupPolygonUnion(p) FROM (
            SELECT arrayJoin([
                readWKTPolygon('POLYGON ((4 0, 4 4, 8 4, 8 0, 4 0))'),
                readWKTPolygon('POLYGON ((2 0, 2 4, 6 4, 6 0, 2 0))'),
                readWKTPolygon('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))')
            ]) AS p
        )) AS u2
);

-- 3. groupPolygonIntersection: forward vs reverse (3 overlapping polygons)
SELECT 'polygon_intersect_forward_vs_reverse';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(i1, i2)), 4) < 0.001 AS equiv
FROM (
    SELECT
        (SELECT groupPolygonIntersection(p) FROM (
            SELECT arrayJoin([
                readWKTPolygon('POLYGON ((0 0, 0 6, 6 6, 6 0, 0 0))'),
                readWKTPolygon('POLYGON ((2 2, 2 8, 8 8, 8 2, 2 2))'),
                readWKTPolygon('POLYGON ((1 1, 1 7, 7 7, 7 1, 1 1))')
            ]) AS p
        )) AS i1,
        (SELECT groupPolygonIntersection(p) FROM (
            SELECT arrayJoin([
                readWKTPolygon('POLYGON ((1 1, 1 7, 7 7, 7 1, 1 1))'),
                readWKTPolygon('POLYGON ((2 2, 2 8, 8 8, 8 2, 2 2))'),
                readWKTPolygon('POLYGON ((0 0, 0 6, 6 6, 6 0, 0 0))')
            ]) AS p
        )) AS i2
);

-- 4. groupPolygonUnion with Geometry variant: forward vs reverse
SELECT 'polygon_union_geometry_forward_vs_reverse';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(u1, u2)), 4) < 0.001 AS equiv
FROM (
    SELECT
        (SELECT groupPolygonUnion(g) FROM (
            SELECT arrayJoin([
                readWKT('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))'),
                readWKT('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'),
                readWKT('POLYGON ((2 2, 2 5, 5 5, 5 2, 2 2))')
            ]) AS g
        )) AS u1,
        (SELECT groupPolygonUnion(g) FROM (
            SELECT arrayJoin([
                readWKT('POLYGON ((2 2, 2 5, 5 5, 5 2, 2 2))'),
                readWKT('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'),
                readWKT('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))')
            ]) AS g
        )) AS u2
);

-- 5. groupPolygonIntersection with Geometry variant: forward vs reverse
SELECT 'polygon_intersect_geometry_forward_vs_reverse';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(i1, i2)), 4) < 0.001 AS equiv
FROM (
    SELECT
        (SELECT groupPolygonIntersection(g) FROM (
            SELECT arrayJoin([
                readWKT('POLYGON ((0 0, 0 6, 6 6, 6 0, 0 0))'),
                readWKT('POLYGON ((2 2, 2 8, 8 8, 8 2, 2 2))'),
                readWKT('POLYGON ((1 1, 1 7, 7 7, 7 1, 1 1))')
            ]) AS g
        )) AS i1,
        (SELECT groupPolygonIntersection(g) FROM (
            SELECT arrayJoin([
                readWKT('POLYGON ((1 1, 1 7, 7 7, 7 1, 1 1))'),
                readWKT('POLYGON ((0 0, 0 6, 6 6, 6 0, 0 0))'),
                readWKT('POLYGON ((2 2, 2 8, 8 8, 8 2, 2 2))')
            ]) AS g
        )) AS i2
);
