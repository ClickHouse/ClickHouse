-- Test State/Merge combinators for geo aggregate functions (exercises serialize/deserialize)

-- groupConvexHull State/Merge
SELECT 'convex_hull_state_merge';
SELECT wkt(groupConvexHullMerge(state)) FROM (
    SELECT groupConvexHullState(pt) AS state FROM (
        SELECT arrayJoin([readWKTPoint('POINT (0 0)'), readWKTPoint('POINT (10 0)')]) AS pt
    )
    UNION ALL
    SELECT groupConvexHullState(pt) AS state FROM (
        SELECT arrayJoin([readWKTPoint('POINT (10 10)'), readWKTPoint('POINT (0 10)')]) AS pt
    )
);

-- Verify area of merged convex hull is correct (should be 100 for a 10x10 square)
SELECT 'convex_hull_merge_area';
SELECT abs(polygonAreaCartesian(groupConvexHullMerge(state)) - 100) < 0.001 FROM (
    SELECT groupConvexHullState(pt) AS state FROM (
        SELECT arrayJoin([readWKTPoint('POINT (0 0)'), readWKTPoint('POINT (10 0)')]) AS pt
    )
    UNION ALL
    SELECT groupConvexHullState(pt) AS state FROM (
        SELECT arrayJoin([readWKTPoint('POINT (10 10)'), readWKTPoint('POINT (0 10)')]) AS pt
    )
);

-- groupPolygonUnion State/Merge
SELECT 'polygon_union_state_merge';
SELECT round(polygonAreaCartesian(groupPolygonUnionMerge(state)), 2) FROM (
    SELECT groupPolygonUnionState(p) AS state FROM (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))') AS p
    )
    UNION ALL
    SELECT groupPolygonUnionState(p) AS state FROM (
        SELECT readWKTPolygon('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))') AS p
    )
);

-- groupPolygonIntersection State/Merge
SELECT 'polygon_intersect_state_merge';
SELECT round(polygonAreaCartesian(groupPolygonIntersectionMerge(state)), 2) FROM (
    SELECT groupPolygonIntersectionState(p) AS state FROM (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))') AS p
    )
    UNION ALL
    SELECT groupPolygonIntersectionState(p) AS state FROM (
        SELECT readWKTPolygon('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))') AS p
    )
);

-- Intersect merge with disjoint -> empty
SELECT 'intersect_merge_disjoint';
SELECT wkt(groupPolygonIntersectionMerge(state)) FROM (
    SELECT groupPolygonIntersectionState(p) AS state FROM (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p
    )
    UNION ALL
    SELECT groupPolygonIntersectionState(p) AS state FROM (
        SELECT readWKTPolygon('POLYGON ((10 10, 10 11, 11 11, 11 10, 10 10))') AS p
    )
);

-- Multiple states merged
SELECT 'multi_state_merge';
SELECT round(polygonAreaCartesian(groupPolygonUnionMerge(state)), 2) FROM (
    SELECT groupPolygonUnionState(p) AS state FROM (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p
    )
    UNION ALL
    SELECT groupPolygonUnionState(p) AS state FROM (
        SELECT readWKTPolygon('POLYGON ((2 2, 2 3, 3 3, 3 2, 2 2))') AS p
    )
    UNION ALL
    SELECT groupPolygonUnionState(p) AS state FROM (
        SELECT readWKTPolygon('POLYGON ((4 4, 4 5, 5 5, 5 4, 4 4))') AS p
    )
);
