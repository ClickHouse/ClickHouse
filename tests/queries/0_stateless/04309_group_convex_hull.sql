-- Test groupConvexHull aggregate function

-- Basic: convex hull of 4 corner points forming a square
SELECT 'square';
SELECT wkt(groupConvexHull(pt)) FROM (
    SELECT arrayJoin([
        readWKTPoint('POINT (0 0)'),
        readWKTPoint('POINT (10 0)'),
        readWKTPoint('POINT (10 10)'),
        readWKTPoint('POINT (0 10)')
    ]) AS pt
);

-- With interior point that should not affect the hull
SELECT 'with_interior_point';
SELECT wkt(groupConvexHull(pt)) FROM (
    SELECT arrayJoin([
        readWKTPoint('POINT (0 0)'),
        readWKTPoint('POINT (10 0)'),
        readWKTPoint('POINT (10 10)'),
        readWKTPoint('POINT (0 10)'),
        readWKTPoint('POINT (5 5)')
    ]) AS pt
);

-- Triangle
SELECT 'triangle';
SELECT wkt(groupConvexHull(pt)) FROM (
    SELECT arrayJoin([
        readWKTPoint('POINT (0 0)'),
        readWKTPoint('POINT (4 0)'),
        readWKTPoint('POINT (2 3)')
    ]) AS pt
);

-- Single point
SELECT 'single_point';
SELECT wkt(groupConvexHull(pt)) FROM (
    SELECT readWKTPoint('POINT (5 5)') AS pt
);

-- All same points
SELECT 'all_same';
SELECT wkt(groupConvexHull(pt)) FROM (
    SELECT arrayJoin([
        readWKTPoint('POINT (1 1)'),
        readWKTPoint('POINT (1 1)'),
        readWKTPoint('POINT (1 1)')
    ]) AS pt
);

-- Collinear points
SELECT 'collinear';
SELECT wkt(groupConvexHull(pt)) FROM (
    SELECT arrayJoin([
        readWKTPoint('POINT (0 0)'),
        readWKTPoint('POINT (1 1)'),
        readWKTPoint('POINT (2 2)'),
        readWKTPoint('POINT (3 3)')
    ]) AS pt
);

-- Empty group
SELECT 'empty_group';
SELECT wkt(groupConvexHull(pt)) FROM (
    SELECT readWKTPoint('POINT (0 0)') AS pt WHERE 0
);

-- From Ring input (all points used)
SELECT 'ring_input';
SELECT wkt(groupConvexHull(r)) FROM (
    SELECT readWKTRing('POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))') AS r
);

-- From Polygon input (only outer ring used)
SELECT 'polygon_input';
SELECT wkt(groupConvexHull(p)) FROM (
    SELECT readWKTPolygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 3 2, 3 3, 2 3, 2 2))') AS p
);

-- From MultiPolygon input (only outer rings used)
SELECT 'multipolygon_input';
SELECT wkt(groupConvexHull(mp)) FROM (
    SELECT readWKTMultiPolygon('MULTIPOLYGON (((0 0, 5 0, 5 5, 0 5, 0 0)), ((10 10, 15 10, 15 15, 10 15, 10 10)))') AS mp
);

-- GROUP BY test
SELECT 'group_by';
SELECT g, wkt(groupConvexHull(pt)) FROM (
    SELECT 1 AS g, readWKTPoint('POINT (0 0)') AS pt
    UNION ALL SELECT 1, readWKTPoint('POINT (1 0)')
    UNION ALL SELECT 1, readWKTPoint('POINT (0 1)')
    UNION ALL SELECT 2, readWKTPoint('POINT (10 10)')
    UNION ALL SELECT 2, readWKTPoint('POINT (20 10)')
    UNION ALL SELECT 2, readWKTPoint('POINT (15 20)')
) GROUP BY g ORDER BY g;

-- Verify area is correct for a known convex hull
SELECT 'area_check';
SELECT abs(polygonAreaCartesian(groupConvexHull(pt)) - 100) < 0.001 FROM (
    SELECT arrayJoin([
        readWKTPoint('POINT (0 0)'),
        readWKTPoint('POINT (10 0)'),
        readWKTPoint('POINT (10 10)'),
        readWKTPoint('POINT (0 10)')
    ]) AS pt
);
