-- Tags: no-fasttest

-- Test groupPolygonIntersection with Ring, Polygon, and MultiPolygon inputs

DROP TABLE IF EXISTS geo;

-- Negative tests: wrong argument type, invalid types
SELECT groupPolygonIntersection(42); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonIntersection((0, 0)); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonIntersection('not a polygon'); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonIntersection(polygon, polygon, polygon) FROM (SELECT [[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]] AS polygon); -- { serverError BAD_ARGUMENTS }

-- Invalid correct_geometry type
SELECT groupPolygonIntersection(polygon, polygon) FROM (SELECT [[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]] AS polygon); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonIntersection(polygon, 1.5) FROM (SELECT [[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]] AS polygon); -- { serverError BAD_ARGUMENTS }

-- Ring input tests
CREATE TABLE geo (ring Ring) ENGINE = Memory();
SELECT 'Ring empty:', wkt(groupPolygonIntersection(ring)) FROM geo;
INSERT INTO geo VALUES ([(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]);
WITH readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)))') AS expected,
     groupPolygonIntersection(ring) AS actual
SELECT 'Ring single:', polygonsEqualsCartesian(actual, expected) FROM geo;
INSERT INTO geo VALUES ([(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]);
WITH readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))') AS expected,
     groupPolygonIntersection(ring) AS actual
SELECT 'Ring two overlapping:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Polygon input tests
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
SELECT 'Polygon empty:', wkt(groupPolygonIntersection(polygon)) FROM geo;
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]);
WITH readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)))') AS expected,
     groupPolygonIntersection(polygon) AS actual
SELECT 'Polygon single:', polygonsEqualsCartesian(actual, expected) FROM geo;
INSERT INTO geo VALUES ([[(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]]);
WITH readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))') AS expected,
     groupPolygonIntersection(polygon) AS actual
SELECT 'Polygon two overlapping:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- MultiPolygon input tests
CREATE TABLE geo (mpoly MultiPolygon) ENGINE = Memory();
SELECT 'MultiPolygon empty:', wkt(groupPolygonIntersection(mpoly)) FROM geo;
INSERT INTO geo VALUES ([[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]]);
WITH readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)))') AS expected,
     groupPolygonIntersection(mpoly) AS actual
SELECT 'MultiPolygon single:', polygonsEqualsCartesian(actual, expected) FROM geo;
INSERT INTO geo VALUES ([[[(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]]]);
WITH readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))') AS expected,
     groupPolygonIntersection(mpoly) AS actual
SELECT 'MultiPolygon two overlapping:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Return type is always MultiPolygon regardless of input type
SELECT 'Ring type:', toTypeName(groupPolygonIntersection(r)), 'Polygon type:', toTypeName(groupPolygonIntersection(p)), 'MultiPolygon type:', toTypeName(groupPolygonIntersection(m))
FROM (SELECT [(0.,0.),(1.,0.),(1.,1.),(0.,0.)] AS r, [[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]] AS p, [[[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]]] AS m);

-- GROUP BY with overlapping groups
CREATE TABLE geo (grp Int32, polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES (1, [[(0, 0), (0, 4), (4, 4), (4, 0), (0, 0)]]);
INSERT INTO geo VALUES (1, [[(1, 1), (1, 5), (5, 5), (5, 1), (1, 1)]]);
INSERT INTO geo VALUES (1, [[(2, 2), (2, 6), (6, 6), (6, 2), (2, 2)]]);
INSERT INTO geo VALUES (2, [[(10, 10), (10, 14), (14, 14), (14, 10), (10, 10)]]);
INSERT INTO geo VALUES (2, [[(11, 11), (11, 15), (15, 15), (15, 11), (11, 11)]]);
WITH readWKTMultiPolygon('MULTIPOLYGON(((2 4,4 4,4 2,2 2,2 4)))') AS expected_grp1,
     readWKTMultiPolygon('MULTIPOLYGON(((11 14,14 14,14 11,11 11,11 14)))') AS expected_grp2
SELECT
    grp,
    polygonsEqualsCartesian(
        groupPolygonIntersection(polygon),
        if(grp = 1, expected_grp1, expected_grp2)
    )
FROM geo
GROUP BY grp
ORDER BY grp;
DROP TABLE geo;

-- Disjoint polygons result in empty intersection
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]]);
INSERT INTO geo VALUES ([[(10, 10), (10, 11), (11, 11), (11, 10), (10, 10)]]);
SELECT 'Disjoint:', wkt(groupPolygonIntersection(polygon)) FROM geo;
DROP TABLE geo;

-- Test correct_geometry parameter (second argument = 1)
CREATE TABLE geo (polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], 1);
INSERT INTO geo VALUES ([[(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]], 1);
WITH readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))') AS expected,
     groupPolygonIntersection(polygon, should_correct) AS actual
SELECT 'correct_geometry=1:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Test correct_geometry parameter with all geometries having same correction setting
CREATE TABLE geo (polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], 0);
INSERT INTO geo VALUES ([[(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]], 0);
WITH readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))') AS expected,
     groupPolygonIntersection(polygon, should_correct) AS actual
SELECT 'correct_geometry=0:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Test correct_geometry parameter with mixed values
CREATE TABLE geo (polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], 1);
INSERT INTO geo VALUES ([[(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]], 0);
WITH readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))') AS expected,
     groupPolygonIntersection(polygon, should_correct) AS actual
SELECT 'correct_geometry mixed:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Three overlapping polygons - intersection of all three
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]]);
INSERT INTO geo VALUES ([[(2, 2), (2, 12), (12, 12), (12, 2), (2, 2)]]);
INSERT INTO geo VALUES ([[(4, 4), (4, 14), (14, 14), (14, 4), (4, 4)]]);
WITH readWKTMultiPolygon('MULTIPOLYGON(((4 10,10 10,10 4,4 4,4 10)))') AS expected,
     groupPolygonIntersection(polygon) AS actual
SELECT 'Three overlapping:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Partial overlap - key test for intersection semantics
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 6), (6, 6), (6, 0), (0, 0)]]);
INSERT INTO geo VALUES ([[(3, 3), (3, 9), (9, 9), (9, 3), (3, 3)]]);
WITH readWKTMultiPolygon('MULTIPOLYGON(((3 6,6 6,6 3,3 3,3 6)))') AS expected,
     groupPolygonIntersection(polygon) AS actual
SELECT 'Partial overlap:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Test with small squares (scaled to integers to avoid floating-point precision issues with polygonsEqualsCartesian)
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]]);
INSERT INTO geo VALUES ([[(5, 5), (5, 15), (15, 15), (15, 5), (5, 5)]]);
WITH readWKTMultiPolygon('MULTIPOLYGON(((5 10,10 10,10 5,5 5,5 10)))') AS expected,
     groupPolygonIntersection(polygon) AS actual
SELECT 'Small squares:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Single geometry with correct_geometry parameter should return unchanged
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]);
WITH readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)))') AS expected,
     groupPolygonIntersection(polygon, 1) AS actual
SELECT 'Single with correct_geometry:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Empty table with correct_geometry parameter
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
SELECT 'Empty with correct_geometry:', wkt(groupPolygonIntersection(polygon, 1)) FROM geo;
DROP TABLE geo;

-- GROUP BY with correct_geometry parameter
CREATE TABLE geo (grp Int32, polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES (1, [[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]], 1);
INSERT INTO geo VALUES (1, [[(2, 2), (2, 12), (12, 12), (12, 2), (2, 2)]], 1);
INSERT INTO geo VALUES (2, [[(5, 5), (5, 15), (15, 15), (15, 5), (5, 5)]], 0);
INSERT INTO geo VALUES (2, [[(7, 7), (7, 17), (17, 17), (17, 7), (7, 7)]], 0);
WITH readWKTMultiPolygon('MULTIPOLYGON(((2 10,10 10,10 2,2 2,2 10)))') AS expected_grp1,
     readWKTMultiPolygon('MULTIPOLYGON(((7 15,15 15,15 7,7 7,7 15)))') AS expected_grp2
SELECT
    grp,
    polygonsEqualsCartesian(
        groupPolygonIntersection(polygon, should_correct),
        if(grp = 1, expected_grp1, expected_grp2)
    )
FROM geo
GROUP BY grp
ORDER BY grp;
DROP TABLE geo;

-- Test with Ring input and correct_geometry
CREATE TABLE geo (ring Ring, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)], 1);
INSERT INTO geo VALUES ([(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)], 1);
WITH readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))') AS expected,
     groupPolygonIntersection(ring, should_correct) AS actual
SELECT 'Ring with correct_geometry:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Test with MultiPolygon input and correct_geometry
CREATE TABLE geo (mpoly MultiPolygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]], 1);
INSERT INTO geo VALUES ([[[(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]]], 1);
WITH readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))') AS expected,
     groupPolygonIntersection(mpoly, should_correct) AS actual
SELECT 'MultiPolygon with correct_geometry:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Geometry (Variant) input: mixed Ring and Polygon
CREATE TABLE geo (g Geometry) ENGINE = Memory();
INSERT INTO geo VALUES ([(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]::Ring::Geometry);
INSERT INTO geo VALUES ([[(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]]::Polygon::Geometry);
WITH readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))') AS expected,
     groupPolygonIntersection(g) AS actual
SELECT 'Geometry Ring+Polygon intersection:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Geometry (Variant) input: mixed Polygon and MultiPolygon
CREATE TABLE geo (g Geometry) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]::Polygon::Geometry);
INSERT INTO geo VALUES ([[[(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]]]::MultiPolygon::Geometry);
WITH readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))') AS expected,
     groupPolygonIntersection(g) AS actual
SELECT 'Geometry Polygon+MultiPolygon intersection:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Geometry (Variant) input: unsupported Point variant should fail
SELECT groupPolygonIntersection(g) FROM (SELECT (0.,0.)::Point::Geometry AS g); -- { serverError BAD_ARGUMENTS }
