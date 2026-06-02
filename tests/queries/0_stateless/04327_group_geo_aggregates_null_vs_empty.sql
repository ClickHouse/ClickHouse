-- Test NULL vs empty geometry semantics for geo aggregate functions.
-- NULL rows should be silently skipped (standard SQL aggregate behavior).
-- Empty polygons: neutral for union, absorbing for intersect.

DROP TABLE IF EXISTS geo_null_test;
CREATE TABLE geo_null_test (g Geometry) ENGINE = Memory;

-- 1. Union: NULL rows are skipped, result is the non-NULL polygon
SELECT 'union_null_skipped';
INSERT INTO geo_null_test VALUES (readWKT('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))'));
INSERT INTO geo_null_test VALUES (NULL);
INSERT INTO geo_null_test VALUES (readWKT('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'));
SELECT round(polygonAreaCartesian(groupPolygonUnion(g)), 2) FROM geo_null_test;
TRUNCATE TABLE geo_null_test;

-- 2. Intersect: NULL rows are skipped, result is intersection of non-NULL polygons
SELECT 'intersect_null_skipped';
INSERT INTO geo_null_test VALUES (readWKT('POLYGON ((0 0, 0 3, 3 3, 3 0, 0 0))'));
INSERT INTO geo_null_test VALUES (NULL);
INSERT INTO geo_null_test VALUES (readWKT('POLYGON ((1 1, 1 4, 4 4, 4 1, 1 1))'));
SELECT round(polygonAreaCartesian(groupPolygonIntersection(g)), 2) FROM geo_null_test;
TRUNCATE TABLE geo_null_test;

-- 3. ConvexHull: NULL rows are skipped
SELECT 'convex_hull_null_skipped';
INSERT INTO geo_null_test VALUES (readWKT('POINT (0 0)'));
INSERT INTO geo_null_test VALUES (NULL);
INSERT INTO geo_null_test VALUES (readWKT('POINT (4 0)'));
INSERT INTO geo_null_test VALUES (NULL);
INSERT INTO geo_null_test VALUES (readWKT('POINT (2 3)'));
SELECT round(polygonAreaCartesian(groupConvexHull(g)), 2) FROM geo_null_test;
TRUNCATE TABLE geo_null_test;

-- 4. Union: all NULLs -> empty result (zero polygons)
SELECT 'union_all_null';
INSERT INTO geo_null_test VALUES (NULL);
INSERT INTO geo_null_test VALUES (NULL);
SELECT length(groupPolygonUnion(g)) FROM geo_null_test;
TRUNCATE TABLE geo_null_test;

-- 5. Intersect: all NULLs -> empty result (zero polygons)
SELECT 'intersect_all_null';
INSERT INTO geo_null_test VALUES (NULL);
INSERT INTO geo_null_test VALUES (NULL);
SELECT length(groupPolygonIntersection(g)) FROM geo_null_test;
TRUNCATE TABLE geo_null_test;

-- 6. ConvexHull: all NULLs -> empty result (zero points)
SELECT 'convex_hull_all_null';
INSERT INTO geo_null_test VALUES (NULL);
INSERT INTO geo_null_test VALUES (NULL);
SELECT length(groupConvexHull(g)) FROM geo_null_test;
TRUNCATE TABLE geo_null_test;

-- 7. Union: NULL + empty polygon -> empty (both skipped/neutral)
SELECT 'union_null_and_empty';
INSERT INTO geo_null_test VALUES (NULL);
INSERT INTO geo_null_test VALUES (readWKT('POLYGON EMPTY'));
SELECT length(groupPolygonUnion(g)) FROM geo_null_test;
TRUNCATE TABLE geo_null_test;

-- 8. Intersect: NULL + empty polygon -> empty (NULL skipped, empty absorbs)
SELECT 'intersect_null_and_empty';
INSERT INTO geo_null_test VALUES (NULL);
INSERT INTO geo_null_test VALUES (readWKT('POLYGON EMPTY'));
SELECT length(groupPolygonIntersection(g)) FROM geo_null_test;
TRUNCATE TABLE geo_null_test;

-- 9. Union: NULL + empty + real polygon -> real polygon
SELECT 'union_null_empty_real';
INSERT INTO geo_null_test VALUES (NULL);
INSERT INTO geo_null_test VALUES (readWKT('POLYGON EMPTY'));
INSERT INTO geo_null_test VALUES (readWKT('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))'));
SELECT round(polygonAreaCartesian(groupPolygonUnion(g)), 2) FROM geo_null_test;
TRUNCATE TABLE geo_null_test;

-- 10. Intersect: NULL + empty + real polygon -> empty (empty absorbs)
SELECT 'intersect_null_empty_real';
INSERT INTO geo_null_test VALUES (NULL);
INSERT INTO geo_null_test VALUES (readWKT('POLYGON EMPTY'));
INSERT INTO geo_null_test VALUES (readWKT('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))'));
SELECT round(polygonAreaCartesian(groupPolygonIntersection(g)), 4) FROM geo_null_test;

DROP TABLE geo_null_test;
