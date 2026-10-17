-- Geo aggregate functions on `Geometry` table columns.

DROP TABLE IF EXISTS geo_agg_test;
CREATE TABLE geo_agg_test (g Geometry) ENGINE = Memory;

-- 1. groupPolygonUnion on Geometry table column
SELECT 'union_geometry_table';
INSERT INTO geo_agg_test VALUES (readWKT('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'));
INSERT INTO geo_agg_test VALUES (readWKT('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))'));
SELECT round(polygonAreaCartesian(groupPolygonUnion(g)), 2) FROM geo_agg_test;
TRUNCATE TABLE geo_agg_test;

-- 2. groupPolygonIntersection on Geometry table column
SELECT 'intersect_geometry_table';
INSERT INTO geo_agg_test VALUES (readWKT('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))'));
INSERT INTO geo_agg_test VALUES (readWKT('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'));
SELECT round(polygonAreaCartesian(groupPolygonIntersection(g)), 2) FROM geo_agg_test;
TRUNCATE TABLE geo_agg_test;

-- 3. groupConvexHull on Geometry table column with Point
SELECT 'convex_hull_geometry_table_points';
INSERT INTO geo_agg_test VALUES (readWKT('POINT (0 0)'));
INSERT INTO geo_agg_test VALUES (readWKT('POINT (4 0)'));
INSERT INTO geo_agg_test VALUES (readWKT('POINT (4 3)'));
INSERT INTO geo_agg_test VALUES (readWKT('POINT (0 3)'));
SELECT round(polygonAreaCartesian(groupConvexHull(g)), 2) FROM geo_agg_test;
TRUNCATE TABLE geo_agg_test;

-- 4. groupConvexHull on Geometry table column with Polygon
SELECT 'convex_hull_geometry_table_polygon';
INSERT INTO geo_agg_test VALUES (readWKT('POLYGON ((0 0, 0 4, 4 4, 4 0, 0 0))'));
INSERT INTO geo_agg_test VALUES (readWKT('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))'));
SELECT round(polygonAreaCartesian(groupConvexHull(g)), 2) FROM geo_agg_test;
TRUNCATE TABLE geo_agg_test;

-- 5. groupPolygonUnion with mixed Polygon and MultiPolygon in Geometry table
SELECT 'union_geometry_table_mixed';
INSERT INTO geo_agg_test VALUES (readWKT('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'));
INSERT INTO geo_agg_test VALUES (readWKT('MULTIPOLYGON (((1 1, 1 3, 3 3, 3 1, 1 1)))'));
SELECT round(polygonAreaCartesian(groupPolygonUnion(g)), 2) FROM geo_agg_test;
TRUNCATE TABLE geo_agg_test;

-- 6. groupConvexHull with mixed Point and LineString in Geometry table
SELECT 'convex_hull_geometry_table_mixed';
INSERT INTO geo_agg_test VALUES (readWKT('POINT (0 0)'));
INSERT INTO geo_agg_test VALUES (readWKT('LINESTRING (5 0, 5 5)'));
INSERT INTO geo_agg_test VALUES (readWKT('POINT (0 5)'));
SELECT round(polygonAreaCartesian(groupConvexHull(g)), 2) FROM geo_agg_test;
TRUNCATE TABLE geo_agg_test;

-- 7. groupPolygonUnion State/Merge round-trip on Geometry table
SELECT 'union_geometry_table_state_merge';
INSERT INTO geo_agg_test VALUES (readWKT('POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))'));
INSERT INTO geo_agg_test VALUES (readWKT('POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))'));
SELECT round(polygonAreaCartesian(groupPolygonUnionMerge(state)), 2) FROM (
    SELECT CAST(unhex(hex(groupPolygonUnionState(g))) AS AggregateFunction(groupPolygonUnion, Geometry)) AS state FROM geo_agg_test
);
TRUNCATE TABLE geo_agg_test;

-- 8. groupPolygonIntersection rejects Point from Geometry table
SELECT 'intersect_rejects_point_geometry_table';
INSERT INTO geo_agg_test VALUES (readWKT('POINT (1 1)'));
SELECT groupPolygonIntersection(g) FROM geo_agg_test; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
TRUNCATE TABLE geo_agg_test;

-- 9. `Geometry` keeps `Ring`/`LineString` and `Polygon`/`MultiLineString` as distinct active types,
--    while each pair has matching underlying array shapes. A closed `LineString` is deliberately
--    aggregated as a `Ring` and a `MultiLineString` as a `Polygon`, so this exercises
--    `normalizePolygonalVariantType` on the table-backed `ColumnVariant` path. The two
--    shapes are the same squares as would be written with POLYGON/MULTIPOLYGON, so the union area
--    is unchanged (a 3x3 square at [0,3] plus a 3x3 square at [2,5], overlapping in [2,3]).
SELECT 'union_linestring_normalization_geometry_table';
INSERT INTO geo_agg_test VALUES (readWKT('LINESTRING (0 0, 0 3, 3 3, 3 0, 0 0)'));
INSERT INTO geo_agg_test VALUES (readWKT('MULTILINESTRING ((2 2, 2 5, 5 5, 5 2, 2 2))'));
SELECT round(polygonAreaCartesian(groupPolygonUnion(g)), 2) FROM geo_agg_test;

DROP TABLE geo_agg_test;
