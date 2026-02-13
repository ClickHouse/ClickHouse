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
SELECT 'Ring single:', wkt(groupPolygonIntersection(ring)) FROM geo;
INSERT INTO geo VALUES ([(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]);
SELECT 'Ring two overlapping:', wkt(groupPolygonIntersection(ring)) FROM geo;
DROP TABLE geo;

-- Polygon input tests
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
SELECT 'Polygon empty:', wkt(groupPolygonIntersection(polygon)) FROM geo;
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]);
SELECT 'Polygon single:', wkt(groupPolygonIntersection(polygon)) FROM geo;
INSERT INTO geo VALUES ([[(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]]);
SELECT 'Polygon two overlapping:', wkt(groupPolygonIntersection(polygon)) FROM geo;
DROP TABLE geo;

-- MultiPolygon input tests
CREATE TABLE geo (mpoly MultiPolygon) ENGINE = Memory();
SELECT 'MultiPolygon empty:', wkt(groupPolygonIntersection(mpoly)) FROM geo;
INSERT INTO geo VALUES ([[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]]);
SELECT 'MultiPolygon single:', wkt(groupPolygonIntersection(mpoly)) FROM geo;
INSERT INTO geo VALUES ([[[(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]]]);
SELECT 'MultiPolygon two overlapping:', wkt(groupPolygonIntersection(mpoly)) FROM geo;
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
SELECT grp, wkt(groupPolygonIntersection(polygon)) FROM geo GROUP BY grp ORDER BY grp;
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
SELECT 'correct_geometry=1:', wkt(groupPolygonIntersection(polygon, should_correct)) FROM geo;
DROP TABLE geo;

-- Test correct_geometry parameter with all geometries having same correction setting
CREATE TABLE geo (polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], 0);
INSERT INTO geo VALUES ([[(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]], 0);
SELECT 'correct_geometry=0:', wkt(groupPolygonIntersection(polygon, should_correct)) FROM geo;
DROP TABLE geo;

-- Test correct_geometry parameter with mixed values
CREATE TABLE geo (polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], 1);
INSERT INTO geo VALUES ([[(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]], 0);
SELECT 'correct_geometry mixed:', wkt(groupPolygonIntersection(polygon, should_correct)) FROM geo;
DROP TABLE geo;

-- Three overlapping polygons - intersection of all three
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]]);
INSERT INTO geo VALUES ([[(2, 2), (2, 12), (12, 12), (12, 2), (2, 2)]]);
INSERT INTO geo VALUES ([[(4, 4), (4, 14), (14, 14), (14, 4), (4, 4)]]);
SELECT 'Three overlapping:', wkt(groupPolygonIntersection(polygon)) FROM geo;
DROP TABLE geo;

-- Partial overlap - key test for intersection semantics
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 6), (6, 6), (6, 0), (0, 0)]]);
INSERT INTO geo VALUES ([[(3, 3), (3, 9), (9, 9), (9, 3), (3, 3)]]);
SELECT 'Partial overlap:', wkt(groupPolygonIntersection(polygon)) FROM geo;
DROP TABLE geo;

-- Test with small squares
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]]);
INSERT INTO geo VALUES ([[(0.5, 0.5), (0.5, 1.5), (1.5, 1.5), (1.5, 0.5), (0.5, 0.5)]]);
SELECT 'Small squares:', wkt(groupPolygonIntersection(polygon)) FROM geo;
DROP TABLE geo;

-- Single geometry with correct_geometry parameter should return unchanged
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]);
SELECT 'Single with correct_geometry:', wkt(groupPolygonIntersection(polygon, 1)) FROM geo;
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
SELECT grp, wkt(groupPolygonIntersection(polygon, should_correct)) FROM geo GROUP BY grp ORDER BY grp;
DROP TABLE geo;

-- Test with Ring input and correct_geometry
CREATE TABLE geo (ring Ring, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)], 1);
INSERT INTO geo VALUES ([(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)], 1);
SELECT 'Ring with correct_geometry:', wkt(groupPolygonIntersection(ring, should_correct)) FROM geo;
DROP TABLE geo;

-- Test with MultiPolygon input and correct_geometry
CREATE TABLE geo (mpoly MultiPolygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]], 1);
INSERT INTO geo VALUES ([[[(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]]], 1);
SELECT 'MultiPolygon with correct_geometry:', wkt(groupPolygonIntersection(mpoly, should_correct)) FROM geo;
DROP TABLE geo;
