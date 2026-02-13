-- Tags: no-fasttest

-- Test groupPolygonUnion with Ring, Polygon, and MultiPolygon inputs

DROP TABLE IF EXISTS geo;

-- Negative tests: wrong argument type, too many arguments
SELECT groupPolygonUnion(42); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonUnion((0, 0)); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonUnion('not a polygon'); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonUnion(polygon, 1, 1) FROM (SELECT [[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]] AS polygon); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Ring input
CREATE TABLE geo (ring Ring) ENGINE = Memory();
SELECT 'Ring empty:', wkt(groupPolygonUnion(ring)) FROM geo;
INSERT INTO geo VALUES ([(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]);
SELECT 'Ring single:', wkt(groupPolygonUnion(ring)) FROM geo;
INSERT INTO geo VALUES ([(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]);
SELECT 'Ring union:', wkt(groupPolygonUnion(ring)) FROM geo;
DROP TABLE geo;

-- Polygon input
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
SELECT 'Polygon empty:', wkt(groupPolygonUnion(polygon)) FROM geo;
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]);
SELECT 'Polygon single:', wkt(groupPolygonUnion(polygon)) FROM geo;
INSERT INTO geo VALUES ([[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]]);
SELECT 'Polygon union:', wkt(groupPolygonUnion(polygon)) FROM geo;
DROP TABLE geo;

-- MultiPolygon input
CREATE TABLE geo (mpoly MultiPolygon) ENGINE = Memory();
SELECT 'MultiPolygon empty:', wkt(groupPolygonUnion(mpoly)) FROM geo;
INSERT INTO geo VALUES ([[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]]);
SELECT 'MultiPolygon single:', wkt(groupPolygonUnion(mpoly)) FROM geo;
INSERT INTO geo VALUES ([[[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]]]);
SELECT 'MultiPolygon union:', wkt(groupPolygonUnion(mpoly)) FROM geo;
DROP TABLE geo;

-- Return type is always MultiPolygon regardless of input type
SELECT 'Ring type:', toTypeName(groupPolygonUnion(r)), 'Polygon type:', toTypeName(groupPolygonUnion(p)), 'MultiPolygon type:', toTypeName(groupPolygonUnion(m))
FROM (SELECT [(0.,0.),(1.,0.),(1.,1.),(0.,0.)] AS r, [[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]] AS p, [[[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]]] AS m);

-- GROUP BY
CREATE TABLE geo (grp Int32, polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES (1, [[(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]]);
INSERT INTO geo VALUES (1, [[(1, 1), (1, 3), (3, 3), (3, 1), (1, 1)]]);
INSERT INTO geo VALUES (2, [[(10, 10), (10, 12), (12, 12), (12, 10), (10, 10)]]);
SELECT grp, wkt(groupPolygonUnion(polygon)) FROM geo GROUP BY grp ORDER BY grp;
DROP TABLE geo;

-- Disjoint polygons remain separate in the result
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]]);
INSERT INTO geo VALUES ([[(10, 10), (10, 11), (11, 11), (11, 10), (10, 10)]]);
SELECT 'Disjoint:', wkt(groupPolygonUnion(polygon)) FROM geo;
DROP TABLE geo;

-- Test correct_geometry parameter (second argument = 1)
CREATE TABLE geo (polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], 1);
INSERT INTO geo VALUES ([[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]], 1);
SELECT 'correct_geometry=1:', wkt(groupPolygonUnion(polygon, should_correct)) FROM geo;
DROP TABLE geo;

-- Test correct_geometry parameter with all geometries having same correction setting
CREATE TABLE geo (polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], 0);
INSERT INTO geo VALUES ([[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]], 0);
SELECT 'correct_geometry=0:', wkt(groupPolygonUnion(polygon, should_correct)) FROM geo;
DROP TABLE geo;

-- Test correct_geometry parameter with mixed values
CREATE TABLE geo (polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], 1);
INSERT INTO geo VALUES ([[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]], 0);
SELECT 'correct_geometry mixed:', wkt(groupPolygonUnion(polygon, should_correct)) FROM geo;
DROP TABLE geo;

-- GROUP BY with correct_geometry parameter
CREATE TABLE geo (grp Int32, polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES (1, [[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], 1);
INSERT INTO geo VALUES (1, [[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]], 1);
INSERT INTO geo VALUES (2, [[(10, 10), (10, 15), (15, 15), (15, 10), (10, 10)]], 0);
INSERT INTO geo VALUES (2, [[(12, 12), (12, 18), (18, 18), (18, 12), (12, 12)]], 0);
SELECT grp, wkt(groupPolygonUnion(polygon, should_correct)) FROM geo GROUP BY grp ORDER BY grp;
DROP TABLE geo;

-- Test with Ring input and correct_geometry
CREATE TABLE geo (ring Ring, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)], 1);
INSERT INTO geo VALUES ([(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)], 1);
SELECT 'Ring with correct_geometry:', wkt(groupPolygonUnion(ring, should_correct)) FROM geo;
DROP TABLE geo;

-- Test with MultiPolygon input and correct_geometry
CREATE TABLE geo (mpoly MultiPolygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]], 1);
INSERT INTO geo VALUES ([[[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]]], 1);
SELECT 'MultiPolygon with correct_geometry:', wkt(groupPolygonUnion(mpoly, should_correct)) FROM geo;
DROP TABLE geo;