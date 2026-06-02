-- Robustness tests for merge trees and serialize/deserialize round-trips.
-- Each case compares direct aggregation, flat merge, and tree merge.
-- State/Merge combinators exercise serialize/deserialize on every boundary.

-- 1. groupConvexHull: 12 points in 4 groups, direct vs flat-merge vs tree-merge
SELECT 'convex_hull_merge_tree';
SELECT
    abs(polygonAreaCartesian(direct) - polygonAreaCartesian(flat_m)) < 0.001 AS flat_area,
    abs(polygonAreaCartesian(direct) - polygonAreaCartesian(tree_m)) < 0.001 AS tree_area,
    abs(polygonPerimeterCartesian(direct) - polygonPerimeterCartesian(flat_m)) < 0.001 AS flat_perim,
    abs(polygonPerimeterCartesian(direct) - polygonPerimeterCartesian(tree_m)) < 0.001 AS tree_perim
FROM (
    SELECT
        (SELECT groupConvexHull(pt) FROM (
            SELECT arrayJoin([
                readWKTPoint('POINT (0 0)'), readWKTPoint('POINT (10 0)'), readWKTPoint('POINT (5 5)'),
                readWKTPoint('POINT (10 10)'), readWKTPoint('POINT (0 10)'), readWKTPoint('POINT (7 3)'),
                readWKTPoint('POINT (2 8)'), readWKTPoint('POINT (8 2)'), readWKTPoint('POINT (4 6)'),
                readWKTPoint('POINT (1 4)'), readWKTPoint('POINT (9 5)'), readWKTPoint('POINT (6 9)')
            ]) AS pt
        )) AS direct,
        -- Flat merge: 4 individual states
        (SELECT groupConvexHullMerge(s) FROM (
            SELECT groupConvexHullState(pt) AS s FROM (
                SELECT arrayJoin([readWKTPoint('POINT (0 0)'), readWKTPoint('POINT (10 0)'), readWKTPoint('POINT (5 5)')]) AS pt
            )
            UNION ALL
            SELECT groupConvexHullState(pt) AS s FROM (
                SELECT arrayJoin([readWKTPoint('POINT (10 10)'), readWKTPoint('POINT (0 10)'), readWKTPoint('POINT (7 3)')]) AS pt
            )
            UNION ALL
            SELECT groupConvexHullState(pt) AS s FROM (
                SELECT arrayJoin([readWKTPoint('POINT (2 8)'), readWKTPoint('POINT (8 2)'), readWKTPoint('POINT (4 6)')]) AS pt
            )
            UNION ALL
            SELECT groupConvexHullState(pt) AS s FROM (
                SELECT arrayJoin([readWKTPoint('POINT (1 4)'), readWKTPoint('POINT (9 5)'), readWKTPoint('POINT (6 9)')]) AS pt
            )
        )) AS flat_m,
        -- Tree merge: pairs (AB, CD), then merge pair-results
        (SELECT groupConvexHullMerge(l2) FROM (
            SELECT groupConvexHullMergeState(l1) AS l2 FROM (
                SELECT groupConvexHullState(pt) AS l1 FROM (
                    SELECT arrayJoin([readWKTPoint('POINT (0 0)'), readWKTPoint('POINT (10 0)'), readWKTPoint('POINT (5 5)')]) AS pt
                )
                UNION ALL
                SELECT groupConvexHullState(pt) AS l1 FROM (
                    SELECT arrayJoin([readWKTPoint('POINT (10 10)'), readWKTPoint('POINT (0 10)'), readWKTPoint('POINT (7 3)')]) AS pt
                )
            )
            UNION ALL
            SELECT groupConvexHullMergeState(l1) AS l2 FROM (
                SELECT groupConvexHullState(pt) AS l1 FROM (
                    SELECT arrayJoin([readWKTPoint('POINT (2 8)'), readWKTPoint('POINT (8 2)'), readWKTPoint('POINT (4 6)')]) AS pt
                )
                UNION ALL
                SELECT groupConvexHullState(pt) AS l1 FROM (
                    SELECT arrayJoin([readWKTPoint('POINT (1 4)'), readWKTPoint('POINT (9 5)'), readWKTPoint('POINT (6 9)')]) AS pt
                )
            )
        )) AS tree_m
);

-- 2. groupPolygonUnion: 8 overlapping polygons in 4 groups of 2, direct vs flat vs tree
SELECT 'union_merge_tree';
SELECT
    round(polygonAreaCartesian(polygonsSymDifferenceCartesian(direct, flat_m)), 4) < 0.001 AS flat_eq,
    round(polygonAreaCartesian(polygonsSymDifferenceCartesian(direct, tree_m)), 4) < 0.001 AS tree_eq
FROM (
    SELECT
        (SELECT groupPolygonUnion(p) FROM (
            SELECT arrayJoin([
                readWKTPolygon('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))'),
                readWKTPolygon('POLYGON ((1 0, 1 3, 4 3, 4 0, 1 0))'),
                readWKTPolygon('POLYGON ((2 0, 2 3, 5 3, 5 0, 2 0))'),
                readWKTPolygon('POLYGON ((3 0, 3 3, 6 3, 6 0, 3 0))'),
                readWKTPolygon('POLYGON ((4 0, 4 3, 7 3, 7 0, 4 0))'),
                readWKTPolygon('POLYGON ((5 0, 5 3, 8 3, 8 0, 5 0))'),
                readWKTPolygon('POLYGON ((6 0, 6 3, 9 3, 9 0, 6 0))'),
                readWKTPolygon('POLYGON ((7 0, 7 3, 10 3, 10 0, 7 0))')
            ]) AS p
        )) AS direct,
        -- Flat merge: 4 states, each with 2 polygons
        (SELECT groupPolygonUnionMerge(s) FROM (
            SELECT groupPolygonUnionState(p) AS s FROM (
                SELECT arrayJoin([
                    readWKTPolygon('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))'),
                    readWKTPolygon('POLYGON ((1 0, 1 3, 4 3, 4 0, 1 0))')
                ]) AS p
            )
            UNION ALL
            SELECT groupPolygonUnionState(p) AS s FROM (
                SELECT arrayJoin([
                    readWKTPolygon('POLYGON ((2 0, 2 3, 5 3, 5 0, 2 0))'),
                    readWKTPolygon('POLYGON ((3 0, 3 3, 6 3, 6 0, 3 0))')
                ]) AS p
            )
            UNION ALL
            SELECT groupPolygonUnionState(p) AS s FROM (
                SELECT arrayJoin([
                    readWKTPolygon('POLYGON ((4 0, 4 3, 7 3, 7 0, 4 0))'),
                    readWKTPolygon('POLYGON ((5 0, 5 3, 8 3, 8 0, 5 0))')
                ]) AS p
            )
            UNION ALL
            SELECT groupPolygonUnionState(p) AS s FROM (
                SELECT arrayJoin([
                    readWKTPolygon('POLYGON ((6 0, 6 3, 9 3, 9 0, 6 0))'),
                    readWKTPolygon('POLYGON ((7 0, 7 3, 10 3, 10 0, 7 0))')
                ]) AS p
            )
        )) AS flat_m,
        -- Tree merge: pair (AB,CD) then merge pairs
        (SELECT groupPolygonUnionMerge(l2) FROM (
            SELECT groupPolygonUnionMergeState(l1) AS l2 FROM (
                SELECT groupPolygonUnionState(p) AS l1 FROM (
                    SELECT arrayJoin([
                        readWKTPolygon('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))'),
                        readWKTPolygon('POLYGON ((1 0, 1 3, 4 3, 4 0, 1 0))')
                    ]) AS p
                )
                UNION ALL
                SELECT groupPolygonUnionState(p) AS l1 FROM (
                    SELECT arrayJoin([
                        readWKTPolygon('POLYGON ((2 0, 2 3, 5 3, 5 0, 2 0))'),
                        readWKTPolygon('POLYGON ((3 0, 3 3, 6 3, 6 0, 3 0))')
                    ]) AS p
                )
            )
            UNION ALL
            SELECT groupPolygonUnionMergeState(l1) AS l2 FROM (
                SELECT groupPolygonUnionState(p) AS l1 FROM (
                    SELECT arrayJoin([
                        readWKTPolygon('POLYGON ((4 0, 4 3, 7 3, 7 0, 4 0))'),
                        readWKTPolygon('POLYGON ((5 0, 5 3, 8 3, 8 0, 5 0))')
                    ]) AS p
                )
                UNION ALL
                SELECT groupPolygonUnionState(p) AS l1 FROM (
                    SELECT arrayJoin([
                        readWKTPolygon('POLYGON ((6 0, 6 3, 9 3, 9 0, 6 0))'),
                        readWKTPolygon('POLYGON ((7 0, 7 3, 10 3, 10 0, 7 0))')
                    ]) AS p
                )
            )
        )) AS tree_m
);

-- 3. groupPolygonIntersection: 6 overlapping polygons in 3 groups of 2, direct vs flat vs tree
SELECT 'intersect_merge_tree';
SELECT
    round(polygonAreaCartesian(polygonsSymDifferenceCartesian(direct, flat_m)), 4) < 0.001 AS flat_eq,
    round(polygonAreaCartesian(polygonsSymDifferenceCartesian(direct, tree_m)), 4) < 0.001 AS tree_eq
FROM (
    SELECT
        (SELECT groupPolygonIntersection(p) FROM (
            SELECT arrayJoin([
                readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'),
                readWKTPolygon('POLYGON ((1 1, 1 9, 9 9, 9 1, 1 1))'),
                readWKTPolygon('POLYGON ((2 0, 2 8, 8 8, 8 0, 2 0))'),
                readWKTPolygon('POLYGON ((0 2, 0 8, 6 8, 6 2, 0 2))'),
                readWKTPolygon('POLYGON ((1 0, 1 7, 7 7, 7 0, 1 0))'),
                readWKTPolygon('POLYGON ((0 1, 0 9, 5 9, 5 1, 0 1))')
            ]) AS p
        )) AS direct,
        -- Flat merge: 3 states, each with 2 polygons
        (SELECT groupPolygonIntersectionMerge(s) FROM (
            SELECT groupPolygonIntersectionState(p) AS s FROM (
                SELECT arrayJoin([
                    readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'),
                    readWKTPolygon('POLYGON ((1 1, 1 9, 9 9, 9 1, 1 1))')
                ]) AS p
            )
            UNION ALL
            SELECT groupPolygonIntersectionState(p) AS s FROM (
                SELECT arrayJoin([
                    readWKTPolygon('POLYGON ((2 0, 2 8, 8 8, 8 0, 2 0))'),
                    readWKTPolygon('POLYGON ((0 2, 0 8, 6 8, 6 2, 0 2))')
                ]) AS p
            )
            UNION ALL
            SELECT groupPolygonIntersectionState(p) AS s FROM (
                SELECT arrayJoin([
                    readWKTPolygon('POLYGON ((1 0, 1 7, 7 7, 7 0, 1 0))'),
                    readWKTPolygon('POLYGON ((0 1, 0 9, 5 9, 5 1, 0 1))')
                ]) AS p
            )
        )) AS flat_m,
        -- Tree merge: pair (AB, C) — 2 + 1
        (SELECT groupPolygonIntersectionMerge(l2) FROM (
            SELECT groupPolygonIntersectionMergeState(l1) AS l2 FROM (
                SELECT groupPolygonIntersectionState(p) AS l1 FROM (
                    SELECT arrayJoin([
                        readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))'),
                        readWKTPolygon('POLYGON ((1 1, 1 9, 9 9, 9 1, 1 1))')
                    ]) AS p
                )
                UNION ALL
                SELECT groupPolygonIntersectionState(p) AS l1 FROM (
                    SELECT arrayJoin([
                        readWKTPolygon('POLYGON ((2 0, 2 8, 8 8, 8 0, 2 0))'),
                        readWKTPolygon('POLYGON ((0 2, 0 8, 6 8, 6 2, 0 2))')
                    ]) AS p
                )
            )
            UNION ALL
            SELECT groupPolygonIntersectionMergeState(l1) AS l2 FROM (
                SELECT groupPolygonIntersectionState(p) AS l1 FROM (
                    SELECT arrayJoin([
                        readWKTPolygon('POLYGON ((1 0, 1 7, 7 7, 7 0, 1 0))'),
                        readWKTPolygon('POLYGON ((0 1, 0 9, 5 9, 5 1, 0 1))')
                    ]) AS p
                )
            )
        )) AS tree_m
);

-- 4. groupPolygonIntersection: early-empty propagation through merge tree
-- Group A is disjoint (empty intersection). Group B is overlapping (non-empty).
-- Both flat and tree merge must produce empty result.
SELECT 'intersect_early_empty_merge_tree';
SELECT
    round(polygonAreaCartesian(flat_m), 4) AS flat_area,
    round(polygonAreaCartesian(tree_m), 4) AS tree_area
FROM (
    SELECT
        (SELECT groupPolygonIntersectionMerge(s) FROM (
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
            UNION ALL
            SELECT groupPolygonIntersectionState(p) AS s FROM (
                SELECT readWKTPolygon('POLYGON ((1 1, 1 9, 9 9, 9 1, 1 1))') AS p
            )
        )) AS flat_m,
        (SELECT groupPolygonIntersectionMerge(l2) FROM (
            -- Group A: disjoint → effectively empty
            SELECT groupPolygonIntersectionMergeState(l1) AS l2 FROM (
                SELECT groupPolygonIntersectionState(p) AS l1 FROM (
                    SELECT readWKTPolygon('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))') AS p
                )
                UNION ALL
                SELECT groupPolygonIntersectionState(p) AS l1 FROM (
                    SELECT readWKTPolygon('POLYGON ((5 5, 5 7, 7 7, 7 5, 5 5))') AS p
                )
            )
            UNION ALL
            -- Group B: overlapping → non-empty
            SELECT groupPolygonIntersectionMergeState(l1) AS l2 FROM (
                SELECT groupPolygonIntersectionState(p) AS l1 FROM (
                    SELECT readWKTPolygon('POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))') AS p
                )
                UNION ALL
                SELECT groupPolygonIntersectionState(p) AS l1 FROM (
                    SELECT readWKTPolygon('POLYGON ((1 1, 1 9, 9 9, 9 1, 1 1))') AS p
                )
            )
        )) AS tree_m
);

-- 5. Stress: 50 overlapping polygons via numbers(), direct vs 5-group merge
SELECT 'union_stress_50';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(direct, merged)), 4) < 0.001 AS equiv
FROM (
    SELECT
        (SELECT groupPolygonUnion(p) FROM (
            SELECT [[(toFloat64(number), 0.), (toFloat64(number), 2.), (toFloat64(number) + 2., 2.), (toFloat64(number) + 2., 0.), (toFloat64(number), 0.)]]::Polygon AS p
            FROM numbers(50)
        )) AS direct,
        (SELECT groupPolygonUnionMerge(s) FROM (
            SELECT groupPolygonUnionState(p) AS s FROM (
                SELECT [[(toFloat64(number), 0.), (toFloat64(number), 2.), (toFloat64(number) + 2., 2.), (toFloat64(number) + 2., 0.), (toFloat64(number), 0.)]]::Polygon AS p
                FROM numbers(50) WHERE number < 10
            )
            UNION ALL
            SELECT groupPolygonUnionState(p) AS s FROM (
                SELECT [[(toFloat64(number), 0.), (toFloat64(number), 2.), (toFloat64(number) + 2., 2.), (toFloat64(number) + 2., 0.), (toFloat64(number), 0.)]]::Polygon AS p
                FROM numbers(50) WHERE number >= 10 AND number < 20
            )
            UNION ALL
            SELECT groupPolygonUnionState(p) AS s FROM (
                SELECT [[(toFloat64(number), 0.), (toFloat64(number), 2.), (toFloat64(number) + 2., 2.), (toFloat64(number) + 2., 0.), (toFloat64(number), 0.)]]::Polygon AS p
                FROM numbers(50) WHERE number >= 20 AND number < 30
            )
            UNION ALL
            SELECT groupPolygonUnionState(p) AS s FROM (
                SELECT [[(toFloat64(number), 0.), (toFloat64(number), 2.), (toFloat64(number) + 2., 2.), (toFloat64(number) + 2., 0.), (toFloat64(number), 0.)]]::Polygon AS p
                FROM numbers(50) WHERE number >= 30 AND number < 40
            )
            UNION ALL
            SELECT groupPolygonUnionState(p) AS s FROM (
                SELECT [[(toFloat64(number), 0.), (toFloat64(number), 2.), (toFloat64(number) + 2., 2.), (toFloat64(number) + 2., 0.), (toFloat64(number), 0.)]]::Polygon AS p
                FROM numbers(50) WHERE number >= 40
            )
        )) AS merged
);

-- 6. Stress: groupConvexHull with 40 points on unit circle, direct vs 4-group merge
SELECT 'convex_hull_stress_40';
SELECT
    abs(polygonAreaCartesian(direct) - polygonAreaCartesian(merged)) < 0.001 AS area_eq,
    abs(polygonPerimeterCartesian(direct) - polygonPerimeterCartesian(merged)) < 0.001 AS perim_eq
FROM (
    SELECT
        (SELECT groupConvexHull(pt) FROM (
            SELECT (cos(toFloat64(number) * 0.15708), sin(toFloat64(number) * 0.15708))::Point AS pt
            FROM numbers(40)
        )) AS direct,
        (SELECT groupConvexHullMerge(s) FROM (
            SELECT groupConvexHullState(pt) AS s FROM (
                SELECT (cos(toFloat64(number) * 0.15708), sin(toFloat64(number) * 0.15708))::Point AS pt
                FROM numbers(40) WHERE number < 10
            )
            UNION ALL
            SELECT groupConvexHullState(pt) AS s FROM (
                SELECT (cos(toFloat64(number) * 0.15708), sin(toFloat64(number) * 0.15708))::Point AS pt
                FROM numbers(40) WHERE number >= 10 AND number < 20
            )
            UNION ALL
            SELECT groupConvexHullState(pt) AS s FROM (
                SELECT (cos(toFloat64(number) * 0.15708), sin(toFloat64(number) * 0.15708))::Point AS pt
                FROM numbers(40) WHERE number >= 20 AND number < 30
            )
            UNION ALL
            SELECT groupConvexHullState(pt) AS s FROM (
                SELECT (cos(toFloat64(number) * 0.15708), sin(toFloat64(number) * 0.15708))::Point AS pt
                FROM numbers(40) WHERE number >= 30
            )
        )) AS merged
);
