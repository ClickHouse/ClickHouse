-- Test State/Merge equivalence and various merge shapes

-- 1. groupConvexHull: direct vs State+Merge must give the same result
SELECT 'convex_hull_direct_vs_state_merge';
SELECT
    abs(polygonAreaCartesian(direct) - polygonAreaCartesian(merged)) < 0.001 AS area_eq,
    abs(polygonPerimeterCartesian(direct) - polygonPerimeterCartesian(merged)) < 0.001 AS perim_eq
FROM (
    SELECT
        (SELECT groupConvexHull(pt) FROM (
            SELECT arrayJoin([
                readWKTPoint('POINT (0 0)'),
                readWKTPoint('POINT (10 0)'),
                readWKTPoint('POINT (10 10)'),
                readWKTPoint('POINT (0 10)'),
                readWKTPoint('POINT (3 7)')
            ]) AS pt
        )) AS direct,
        (SELECT groupConvexHullMerge(state) FROM (
            SELECT groupConvexHullState(pt) AS state FROM (
                SELECT arrayJoin([readWKTPoint('POINT (0 0)'), readWKTPoint('POINT (10 0)')]) AS pt
            )
            UNION ALL
            SELECT groupConvexHullState(pt) AS state FROM (
                SELECT arrayJoin([readWKTPoint('POINT (10 10)'), readWKTPoint('POINT (0 10)'), readWKTPoint('POINT (3 7)')]) AS pt
            )
        )) AS merged
);

-- 2. groupPolygonUnion: direct vs State+Merge
SELECT 'polygon_union_direct_vs_state_merge';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(direct, merged)), 4) < 0.001 AS equiv
FROM (
    SELECT
        (SELECT groupPolygonUnion(p) FROM (
            SELECT arrayJoin([
                readWKTPolygon('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))'),
                readWKTPolygon('POLYGON ((2 2, 2 5, 5 5, 5 2, 2 2))')
            ]) AS p
        )) AS direct,
        (SELECT groupPolygonUnionMerge(state) FROM (
            SELECT groupPolygonUnionState(p) AS state FROM (
                SELECT readWKTPolygon('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))') AS p
            )
            UNION ALL
            SELECT groupPolygonUnionState(p) AS state FROM (
                SELECT readWKTPolygon('POLYGON ((2 2, 2 5, 5 5, 5 2, 2 2))') AS p
            )
        )) AS merged
);

-- 3. groupPolygonIntersection: direct vs State+Merge
SELECT 'polygon_intersect_direct_vs_state_merge';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(direct, merged)), 4) < 0.001 AS equiv
FROM (
    SELECT
        (SELECT groupPolygonIntersection(p) FROM (
            SELECT arrayJoin([
                readWKTPolygon('POLYGON ((0 0, 0 6, 6 6, 6 0, 0 0))'),
                readWKTPolygon('POLYGON ((2 2, 2 8, 8 8, 8 2, 2 2))')
            ]) AS p
        )) AS direct,
        (SELECT groupPolygonIntersectionMerge(state) FROM (
            SELECT groupPolygonIntersectionState(p) AS state FROM (
                SELECT readWKTPolygon('POLYGON ((0 0, 0 6, 6 6, 6 0, 0 0))') AS p
            )
            UNION ALL
            SELECT groupPolygonIntersectionState(p) AS state FROM (
                SELECT readWKTPolygon('POLYGON ((2 2, 2 8, 8 8, 8 2, 2 2))') AS p
            )
        )) AS merged
);

-- 4. groupPolygonUnion: many states merged (3-way split)
SELECT 'polygon_union_many_states_merge';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(direct, merged)), 4) < 0.001 AS equiv
FROM (
    SELECT
        (SELECT groupPolygonUnion(p) FROM (
            SELECT arrayJoin([
                readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'),
                readWKTPolygon('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))'),
                readWKTPolygon('POLYGON ((2 2, 2 4, 4 4, 4 2, 2 2))')
            ]) AS p
        )) AS direct,
        (SELECT groupPolygonUnionMerge(state) FROM (
            SELECT groupPolygonUnionState(p) AS state FROM (
                SELECT readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))') AS p
            )
            UNION ALL
            SELECT groupPolygonUnionState(p) AS state FROM (
                SELECT readWKTPolygon('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))') AS p
            )
            UNION ALL
            SELECT groupPolygonUnionState(p) AS state FROM (
                SELECT readWKTPolygon('POLYGON ((2 2, 2 4, 4 4, 4 2, 2 2))') AS p
            )
        )) AS merged
);

-- 5. groupPolygonIntersection: many states merged (3-way split)
SELECT 'polygon_intersect_many_states_merge';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(direct, merged)), 4) < 0.001 AS equiv
FROM (
    SELECT
        (SELECT groupPolygonIntersection(p) FROM (
            SELECT arrayJoin([
                readWKTPolygon('POLYGON ((0 0, 0 6, 6 6, 6 0, 0 0))'),
                readWKTPolygon('POLYGON ((2 2, 2 8, 8 8, 8 2, 2 2))'),
                readWKTPolygon('POLYGON ((1 1, 1 7, 7 7, 7 1, 1 1))')
            ]) AS p
        )) AS direct,
        (SELECT groupPolygonIntersectionMerge(state) FROM (
            SELECT groupPolygonIntersectionState(p) AS state FROM (
                SELECT readWKTPolygon('POLYGON ((0 0, 0 6, 6 6, 6 0, 0 0))') AS p
            )
            UNION ALL
            SELECT groupPolygonIntersectionState(p) AS state FROM (
                SELECT readWKTPolygon('POLYGON ((2 2, 2 8, 8 8, 8 2, 2 2))') AS p
            )
            UNION ALL
            SELECT groupPolygonIntersectionState(p) AS state FROM (
                SELECT readWKTPolygon('POLYGON ((1 1, 1 7, 7 7, 7 1, 1 1))') AS p
            )
        )) AS merged
);

-- 6. groupPolygonIntersection: disjoint states merged in different order
SELECT 'intersect_merge_disjoint_order_variants';
SELECT
    round(polygonAreaCartesian(r1), 4) AS area1,
    round(polygonAreaCartesian(r2), 4) AS area2
FROM (
    SELECT
        (SELECT groupPolygonIntersectionMerge(state) FROM (
            SELECT groupPolygonIntersectionState(p) AS state FROM (
                SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p
            )
            UNION ALL
            SELECT groupPolygonIntersectionState(p) AS state FROM (
                SELECT readWKTPolygon('POLYGON ((10 10, 10 11, 11 11, 11 10, 10 10))') AS p
            )
        )) AS r1,
        (SELECT groupPolygonIntersectionMerge(state) FROM (
            SELECT groupPolygonIntersectionState(p) AS state FROM (
                SELECT readWKTPolygon('POLYGON ((10 10, 10 11, 11 11, 11 10, 10 10))') AS p
            )
            UNION ALL
            SELECT groupPolygonIntersectionState(p) AS state FROM (
                SELECT readWKTPolygon('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS p
            )
        )) AS r2
);

-- 7. Merge with empty state (single polygon union should return itself)
SELECT 'merge_with_single_state';
SELECT round(polygonAreaCartesian(groupPolygonUnionMerge(state)), 2) FROM (
    SELECT groupPolygonUnionState(p) AS state FROM (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))') AS p
    )
);
