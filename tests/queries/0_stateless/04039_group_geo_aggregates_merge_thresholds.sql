-- Test merge-side threshold paths (UNION_REDUCTION_THRESHOLD=16, INTERSECT_REDUCTION_THRESHOLD=8)

-- 1. Union: 17 individual states merged must equal direct result
SELECT 'polygon_union_merge_threshold_equivalence';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(direct, merged)), 4) < 0.001 AS equiv
FROM (
    SELECT
        (SELECT groupPolygonUnion(p) FROM (
            SELECT [[(toFloat64(number) * 0.5, 0.), (toFloat64(number) * 0.5, 1.), (toFloat64(number) * 0.5 + 1., 1.), (toFloat64(number) * 0.5 + 1., 0.), (toFloat64(number) * 0.5, 0.)]]::Polygon AS p
            FROM numbers(17)
        )) AS direct,
        (SELECT groupPolygonUnionMerge(state) FROM (
            SELECT groupPolygonUnionState(p) AS state
            FROM (
                SELECT [[(toFloat64(number) * 0.5, 0.), (toFloat64(number) * 0.5, 1.), (toFloat64(number) * 0.5 + 1., 1.), (toFloat64(number) * 0.5 + 1., 0.), (toFloat64(number) * 0.5, 0.)]]::Polygon AS p, number AS grp
                FROM numbers(17)
            ) GROUP BY grp
        )) AS merged
);

-- 2. Intersect: 9 individual states merged must equal direct result
SELECT 'polygon_intersect_merge_threshold_equivalence';
SELECT round(polygonAreaCartesian(polygonsSymDifferenceCartesian(direct, merged)), 4) < 0.001 AS equiv
FROM (
    SELECT
        (SELECT groupPolygonIntersection(p) FROM (
            SELECT [[(toFloat64(number) * 0.25, toFloat64(number) * 0.25), (toFloat64(number) * 0.25, toFloat64(number) * 0.25 + 10.), (toFloat64(number) * 0.25 + 10., toFloat64(number) * 0.25 + 10.), (toFloat64(number) * 0.25 + 10., toFloat64(number) * 0.25), (toFloat64(number) * 0.25, toFloat64(number) * 0.25)]]::Polygon AS p
            FROM numbers(9)
        )) AS direct,
        (SELECT groupPolygonIntersectionMerge(state) FROM (
            SELECT groupPolygonIntersectionState(p) AS state
            FROM (
                SELECT [[(toFloat64(number) * 0.25, toFloat64(number) * 0.25), (toFloat64(number) * 0.25, toFloat64(number) * 0.25 + 10.), (toFloat64(number) * 0.25 + 10., toFloat64(number) * 0.25 + 10.), (toFloat64(number) * 0.25 + 10., toFloat64(number) * 0.25), (toFloat64(number) * 0.25, toFloat64(number) * 0.25)]]::Polygon AS p, number AS grp
                FROM numbers(9)
            ) GROUP BY grp
        )) AS merged
);
