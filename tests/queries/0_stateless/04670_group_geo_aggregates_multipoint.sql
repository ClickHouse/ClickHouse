-- `MultiPoint` inputs: accepted by `groupConvexHull` (every point contributes),
-- rejected by the polygonal aggregates, both for typed columns and for an active
-- `MultiPoint` value inside a `Geometry` column.

DROP TABLE IF EXISTS geo_multipoint_test;
CREATE TABLE geo_multipoint_test (mp MultiPoint) ENGINE = Memory;

INSERT INTO geo_multipoint_test VALUES ([(0., 0.)::Point, (4., 0.)::Point]);
INSERT INTO geo_multipoint_test VALUES ([(4., 3.)::Point, (0., 3.)::Point, (2., 1.)::Point]);

SELECT 'convex_hull_typed_multipoint';
SELECT round(polygonAreaCartesian(groupConvexHull(mp)), 2) FROM geo_multipoint_test;

SELECT 'union_typed_multipoint_rejected';
SELECT groupPolygonUnion(mp) FROM geo_multipoint_test; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'intersect_typed_multipoint_rejected';
SELECT groupPolygonIntersection(mp) FROM geo_multipoint_test; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE geo_multipoint_test;

DROP TABLE IF EXISTS geo_multipoint_geometry_test;
CREATE TABLE geo_multipoint_geometry_test (g Geometry) ENGINE = Memory;

INSERT INTO geo_multipoint_geometry_test VALUES (readWKT('MULTIPOINT ((0 0), (4 0))'));
INSERT INTO geo_multipoint_geometry_test VALUES (readWKT('MULTIPOINT ((4 3), (0 3), (2 1))'));

SELECT 'convex_hull_geometry_multipoint';
SELECT round(polygonAreaCartesian(groupConvexHull(g)), 2) FROM geo_multipoint_geometry_test;

SELECT 'union_geometry_multipoint_rejected';
SELECT groupPolygonUnion(g) FROM geo_multipoint_geometry_test; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'intersect_geometry_multipoint_rejected';
SELECT groupPolygonIntersection(g) FROM geo_multipoint_geometry_test; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE geo_multipoint_geometry_test;
