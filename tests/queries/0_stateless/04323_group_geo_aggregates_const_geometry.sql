-- Test const/literal Geometry paths (ColumnConst wrapping ColumnVariant)

-- 1. Const Geometry Point through groupConvexHull
SELECT 'const_convex_hull_point';
SELECT wkt(groupConvexHull(g)) FROM (
    SELECT readWKT('POINT (3 4)') AS g
    FROM numbers(3)
);

-- 2. Const Geometry Polygon through groupConvexHull
SELECT 'const_convex_hull_polygon';
SELECT round(polygonAreaCartesian(groupConvexHull(g)), 2) FROM (
    SELECT readWKT('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))') AS g
    FROM numbers(3)
);

-- 3. Const Geometry Polygon through groupPolygonUnion
SELECT 'const_union_polygon';
SELECT round(polygonAreaCartesian(groupPolygonUnion(g)), 2) FROM (
    SELECT readWKT('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))') AS g
    FROM numbers(5)
);

-- 4. Const Geometry Polygon through groupPolygonIntersection
SELECT 'const_intersect_polygon';
SELECT round(polygonAreaCartesian(groupPolygonIntersection(g)), 2) FROM (
    SELECT readWKT('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))') AS g
    FROM numbers(5)
);

-- 5. Const Geometry with State/Merge round-trip
SELECT 'const_convex_hull_state_merge';
SELECT
    abs(polygonAreaCartesian(direct) - polygonAreaCartesian(merged)) < 0.001 AS area_eq
FROM (
    SELECT
        (SELECT groupConvexHull(g) FROM (
            SELECT readWKT('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))') AS g
            FROM numbers(3)
        )) AS direct,
        (SELECT groupConvexHullMerge(state) FROM (
            SELECT groupConvexHullState(g) AS state FROM (
                SELECT readWKT('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))') AS g
            )
            UNION ALL
            SELECT groupConvexHullState(g) AS state FROM (
                SELECT readWKT('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))') AS g
                FROM numbers(2)
            )
        )) AS merged
);
