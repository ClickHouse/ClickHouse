-- Test pairwise reduction with odd numbers of chunks

-- 1. Union: 5 states (odd) -> pairwise reduction handles tail correctly
SELECT 'union_5_states_odd';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(direct, merged)), 4) < 0.001 AS equiv
FROM (
    SELECT
        (SELECT groupPolygonUnion(p) FROM (
            SELECT arrayJoin([
                readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'),
                readWKTPolygon('POLYGON ((1 0, 1 2, 3 2, 3 0, 1 0))'),
                readWKTPolygon('POLYGON ((2 0, 2 2, 4 2, 4 0, 2 0))'),
                readWKTPolygon('POLYGON ((3 0, 3 2, 5 2, 5 0, 3 0))'),
                readWKTPolygon('POLYGON ((4 0, 4 2, 6 2, 6 0, 4 0))')
            ]) AS p
        )) AS direct,
        (SELECT groupPolygonUnionMerge(s) FROM (
            SELECT groupPolygonUnionState(p) AS s FROM (
                SELECT readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))') AS p
            )
            UNION ALL
            SELECT groupPolygonUnionState(p) AS s FROM (
                SELECT readWKTPolygon('POLYGON ((1 0, 1 2, 3 2, 3 0, 1 0))') AS p
            )
            UNION ALL
            SELECT groupPolygonUnionState(p) AS s FROM (
                SELECT readWKTPolygon('POLYGON ((2 0, 2 2, 4 2, 4 0, 2 0))') AS p
            )
            UNION ALL
            SELECT groupPolygonUnionState(p) AS s FROM (
                SELECT readWKTPolygon('POLYGON ((3 0, 3 2, 5 2, 5 0, 3 0))') AS p
            )
            UNION ALL
            SELECT groupPolygonUnionState(p) AS s FROM (
                SELECT readWKTPolygon('POLYGON ((4 0, 4 2, 6 2, 6 0, 4 0))') AS p
            )
        )) AS merged
);

-- 2. Intersect: 5 states (odd), non-trivial intersection
SELECT 'intersect_5_states_odd';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(direct, merged)), 4) < 0.001 AS equiv
FROM (
    SELECT
        (SELECT groupPolygonIntersection(p) FROM (
            SELECT arrayJoin([
                readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'),
                readWKTPolygon('POLYGON ((1 1, 1 9, 9 9, 9 1, 1 1))'),
                readWKTPolygon('POLYGON ((2 0, 2 8, 8 8, 8 0, 2 0))'),
                readWKTPolygon('POLYGON ((0 2, 0 8, 7 8, 7 2, 0 2))'),
                readWKTPolygon('POLYGON ((1 0, 1 7, 6 7, 6 0, 1 0))')
            ]) AS p
        )) AS direct,
        (SELECT groupPolygonIntersectionMerge(s) FROM (
            SELECT groupPolygonIntersectionState(p) AS s FROM (
                SELECT readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))') AS p
            )
            UNION ALL
            SELECT groupPolygonIntersectionState(p) AS s FROM (
                SELECT readWKTPolygon('POLYGON ((1 1, 1 9, 9 9, 9 1, 1 1))') AS p
            )
            UNION ALL
            SELECT groupPolygonIntersectionState(p) AS s FROM (
                SELECT readWKTPolygon('POLYGON ((2 0, 2 8, 8 8, 8 0, 2 0))') AS p
            )
            UNION ALL
            SELECT groupPolygonIntersectionState(p) AS s FROM (
                SELECT readWKTPolygon('POLYGON ((0 2, 0 8, 7 8, 7 2, 0 2))') AS p
            )
            UNION ALL
            SELECT groupPolygonIntersectionState(p) AS s FROM (
                SELECT readWKTPolygon('POLYGON ((1 0, 1 7, 6 7, 6 0, 1 0))') AS p
            )
        )) AS merged
);

-- 3. Intersect: 3 states (odd), with early-empty in pairwise tree
SELECT 'intersect_3_states_early_empty';
SELECT round(polygonAreaCartesian(groupPolygonIntersectionMerge(s)), 4) FROM (
    SELECT groupPolygonIntersectionState(p) AS s FROM (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))') AS p
    )
    UNION ALL
    SELECT groupPolygonIntersectionState(p) AS s FROM (
        SELECT readWKTPolygon('POLYGON ((5 5, 5 7, 7 7, 7 5, 5 5))') AS p
    )
    UNION ALL
    SELECT groupPolygonIntersectionState(p) AS s FROM (
        SELECT readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))') AS p
    )
);

-- 4. ConvexHull: 5 groups (odd), direct vs merge
SELECT 'convex_hull_5_groups_odd';
SELECT
    abs(polygonAreaCartesian(direct) - polygonAreaCartesian(merged)) < 0.001 AS area_eq
FROM (
    SELECT
        (SELECT groupConvexHull(pt) FROM (
            SELECT arrayJoin([
                readWKTPoint('POINT (0 0)'),
                readWKTPoint('POINT (10 0)'),
                readWKTPoint('POINT (10 10)'),
                readWKTPoint('POINT (0 10)'),
                readWKTPoint('POINT (5 5)')
            ]) AS pt
        )) AS direct,
        (SELECT groupConvexHullMerge(s) FROM (
            SELECT groupConvexHullState(pt) AS s FROM (SELECT readWKTPoint('POINT (0 0)') AS pt)
            UNION ALL
            SELECT groupConvexHullState(pt) AS s FROM (SELECT readWKTPoint('POINT (10 0)') AS pt)
            UNION ALL
            SELECT groupConvexHullState(pt) AS s FROM (SELECT readWKTPoint('POINT (10 10)') AS pt)
            UNION ALL
            SELECT groupConvexHullState(pt) AS s FROM (SELECT readWKTPoint('POINT (0 10)') AS pt)
            UNION ALL
            SELECT groupConvexHullState(pt) AS s FROM (SELECT readWKTPoint('POINT (5 5)') AS pt)
        )) AS merged
);
