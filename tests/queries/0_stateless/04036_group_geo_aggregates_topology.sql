-- Test complex topology: edge/vertex touching, holes, multi-component results

-- 1. Union of edge-touching polygons (share an edge, union should be seamless)
SELECT 'union_edge_touching';
SELECT round(polygonAreaCartesian(groupPolygonUnion(p)), 2) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'),
        readWKTPolygon('POLYGON ((2 0, 2 2, 4 2, 4 0, 2 0))')
    ]) AS p
);

-- 2. Union of vertex-touching polygons (share only a corner point)
SELECT 'union_vertex_touching';
SELECT round(polygonAreaCartesian(groupPolygonUnion(p)), 2) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'),
        readWKTPolygon('POLYGON ((2 2, 2 4, 4 4, 4 2, 2 2))')
    ]) AS p
);

-- 3. Union of disjoint polygons (no overlap, multi-component result)
SELECT 'union_disjoint';
SELECT round(polygonAreaCartesian(groupPolygonUnion(p)), 2) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))'),
        readWKTPolygon('POLYGON ((5 5, 5 6, 6 6, 6 5, 5 5))')
    ]) AS p
);

-- 4. Intersect of edge-touching polygons (shared edge -> degenerate/zero area)
SELECT 'intersect_edge_touching';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(p)), 4) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'),
        readWKTPolygon('POLYGON ((2 0, 2 2, 4 2, 4 0, 2 0))')
    ]) AS p
);

-- 5. Intersect of fully contained polygon
SELECT 'intersect_contained';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(p)), 2) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'),
        readWKTPolygon('POLYGON ((2 2, 2 5, 5 5, 5 2, 2 2))')
    ]) AS p
);

-- 6. Convex hull of collinear points (degenerate hull)
SELECT 'convex_hull_collinear';
SELECT polygonPerimeterCartesian(groupConvexHull(pt)) > 0 AS has_perimeter FROM (
    SELECT arrayJoin([
        readWKTPoint('POINT (0 0)'),
        readWKTPoint('POINT (5 0)'),
        readWKTPoint('POINT (10 0)')
    ]) AS pt
);

-- 7. Union with polygon that has a hole
SELECT 'union_with_hole';
SELECT round(polygonAreaCartesian(groupPolygonUnion(p)), 2) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3))'),
        readWKTPolygon('POLYGON ((4 4, 4 6, 6 6, 6 4, 4 4))')
    ]) AS p
);

-- 8. Intersect of two overlapping polygons where one has a hole
SELECT 'intersect_with_hole';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(p)), 2) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3))'),
        readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))')
    ]) AS p
);

-- 9. Convex hull with single point
SELECT 'convex_hull_single_point';
SELECT wkt(groupConvexHull(pt)) FROM (
    SELECT readWKTPoint('POINT (3 4)') AS pt
);

-- 10. Convex hull with two points
SELECT 'convex_hull_two_points';
SELECT polygonPerimeterCartesian(groupConvexHull(pt)) > 0 AS has_perimeter FROM (
    SELECT arrayJoin([
        readWKTPoint('POINT (0 0)'),
        readWKTPoint('POINT (5 5)')
    ]) AS pt
);

-- 11. Intersect of vertex-touching polygons (share only a corner -> zero area)
SELECT 'intersect_vertex_touching';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(p)), 4) FROM (
    SELECT arrayJoin([
        readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'),
        readWKTPolygon('POLYGON ((2 2, 2 4, 4 4, 4 2, 2 2))')
    ]) AS p
);

-- 12. Union filling a hole: geometric equivalence via symDifference
SELECT 'union_fills_hole_geometric_equivalence';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(
    actual,
    [[[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)]]]::MultiPolygon
)), 4) < 0.001 AS equiv
FROM (
    SELECT groupPolygonUnion(p) AS actual FROM (
        SELECT arrayJoin([
            readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3))'),
            readWKTPolygon('POLYGON ((3 3, 3 7, 7 7, 7 3, 3 3))')
        ]) AS p
    )
);
