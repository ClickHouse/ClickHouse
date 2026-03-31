-- Test error cases for Geometry variant with non-polygonal types

-- 1. groupPolygonUnion rejects Point geometry
SELECT 'union_rejects_point';
SELECT groupPolygonUnion(g) FROM (
    SELECT readWKT('POINT (1 1)') AS g
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- 2. groupPolygonIntersection rejects Point geometry
SELECT 'intersect_rejects_point';
SELECT groupPolygonIntersection(g) FROM (
    SELECT readWKT('POINT (1 1)') AS g
); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- 3. LineString Geometry values are structurally identical to Ring,
--    so the Variant column conflates them. Polygon functions accept
--    the data (treated as Ring).
SELECT 'linestring_geometry_treated_as_ring';
SELECT round(polygonAreaCartesian(groupPolygonUnion(g)), 2) FROM (
    SELECT readWKT('LINESTRING (0 0, 2 0, 2 2, 0 2, 0 0)') AS g
);

-- 4. MultiLineString Geometry values are structurally identical to Polygon,
--    so the Variant column conflates them. Polygon functions accept
--    the data (treated as Polygon).
SELECT 'multilinestring_geometry_treated_as_polygon';
SELECT round(polygonAreaCartesian(groupPolygonUnion(g)), 2) FROM (
    SELECT readWKT('MULTILINESTRING ((0 0, 2 0, 2 2, 0 2, 0 0))') AS g
);

-- 4b. LineString Geometry treated as Ring for groupPolygonIntersection
SELECT 'linestring_geometry_intersect';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(g)), 2) FROM (
    SELECT arrayJoin([
        readWKT('LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0)'),
        readWKT('LINESTRING (1 1, 3 1, 3 3, 1 3, 1 1)')
    ]) AS g
);

-- 4c. MultiLineString Geometry treated as Polygon for groupPolygonIntersection
SELECT 'multilinestring_geometry_intersect';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(g)), 2) FROM (
    SELECT arrayJoin([
        readWKT('MULTILINESTRING ((0 0, 4 0, 4 4, 0 4, 0 0))'),
        readWKT('MULTILINESTRING ((1 1, 3 1, 3 3, 1 3, 1 1))')
    ]) AS g
);

-- 5. groupConvexHull rejects NaN coordinates
SELECT 'convex_hull_rejects_nan';
SELECT groupConvexHull(pt) FROM (
    SELECT arrayJoin([readWKTPoint('POINT (0 0)'), tuple(nan, 0)::Point]) AS pt
); -- { serverError BAD_ARGUMENTS }

-- 6. groupPolygonUnion rejects NaN coordinates
SELECT 'union_rejects_nan';
SELECT groupPolygonUnion(p) FROM (
    SELECT [[(nan, 0.), (1., 0.), (1., 1.), (0., 1.), (nan, 0.)]]::Polygon AS p
); -- { serverError BAD_ARGUMENTS }

-- 7. groupPolygonIntersection rejects NaN coordinates
SELECT 'intersect_rejects_nan';
SELECT groupPolygonIntersection(p) FROM (
    SELECT [[(nan, 0.), (1., 0.), (1., 1.), (0., 1.), (nan, 0.)]]::Polygon AS p
); -- { serverError BAD_ARGUMENTS }

-- 8. groupPolygonUnion rejects Inf coordinates
SELECT 'union_rejects_inf';
SELECT groupPolygonUnion(p) FROM (
    SELECT [[(inf, 0.), (1., 0.), (1., 1.), (0., 1.), (inf, 0.)]]::Polygon AS p
); -- { serverError BAD_ARGUMENTS }

-- 9. groupConvexHull rejects Inf coordinates
SELECT 'convex_hull_rejects_inf';
SELECT groupConvexHull(pt) FROM (
    SELECT arrayJoin([readWKTPoint('POINT (0 0)'), tuple(inf, 0.)::Point]) AS pt
); -- { serverError BAD_ARGUMENTS }

-- 10. groupPolygonIntersection rejects Inf coordinates
SELECT 'intersect_rejects_inf';
SELECT groupPolygonIntersection(p) FROM (
    SELECT [[(inf, 0.), (1., 0.), (1., 1.), (0., 1.), (inf, 0.)]]::Polygon AS p
); -- { serverError BAD_ARGUMENTS }

-- 11. groupPolygonUnion rejects NaN through Geometry variant
SELECT 'union_rejects_nan_geometry';
SELECT groupPolygonUnion(g) FROM (
    SELECT [[(nan, 0.), (1., 0.), (1., 1.), (0., 1.), (nan, 0.)]]::Polygon::Geometry AS g
); -- { serverError BAD_ARGUMENTS }

-- 12. groupPolygonIntersection rejects Inf through Geometry variant
SELECT 'intersect_rejects_inf_geometry';
SELECT groupPolygonIntersection(g) FROM (
    SELECT [[(inf, 0.), (1., 0.), (1., 1.), (0., 1.), (inf, 0.)]]::Polygon::Geometry AS g
); -- { serverError BAD_ARGUMENTS }
