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
WITH
    groupPolygonUnion(ring) AS actual,
    readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)))') AS expected
SELECT 'Ring single:', polygonsEqualsCartesian(actual, expected) FROM geo;
INSERT INTO geo VALUES ([(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]);
WITH
    groupPolygonUnion(ring) AS actual,
    readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))') AS expected
SELECT 'Ring union:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Polygon input
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
SELECT 'Polygon empty:', wkt(groupPolygonUnion(polygon)) FROM geo;
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]);
WITH
    groupPolygonUnion(polygon) AS actual,
    readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)))') AS expected
SELECT 'Polygon single:', polygonsEqualsCartesian(actual, expected) FROM geo;
INSERT INTO geo VALUES ([[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]]);
WITH
    groupPolygonUnion(polygon) AS actual,
    readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))') AS expected
SELECT 'Polygon union:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- MultiPolygon input
CREATE TABLE geo (mpoly MultiPolygon) ENGINE = Memory();
SELECT 'MultiPolygon empty:', wkt(groupPolygonUnion(mpoly)) FROM geo;
INSERT INTO geo VALUES ([[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]]);
WITH
    groupPolygonUnion(mpoly) AS actual,
    readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)))') AS expected
SELECT 'MultiPolygon single:', polygonsEqualsCartesian(actual, expected) FROM geo;
INSERT INTO geo VALUES ([[[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]]]);
WITH
    groupPolygonUnion(mpoly) AS actual,
    readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))') AS expected
SELECT 'MultiPolygon union:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Return type is always MultiPolygon regardless of input type
SELECT 'Ring type:', toTypeName(groupPolygonUnion(r)), 'Polygon type:', toTypeName(groupPolygonUnion(p)), 'MultiPolygon type:', toTypeName(groupPolygonUnion(m))
FROM (SELECT [(0.,0.),(1.,0.),(1.,1.),(0.,0.)] AS r, [[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]] AS p, [[[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]]] AS m);

-- GROUP BY
CREATE TABLE geo (grp Int32, polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES (1, [[(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]]);
INSERT INTO geo VALUES (1, [[(1, 1), (1, 3), (3, 3), (3, 1), (1, 1)]]);
INSERT INTO geo VALUES (2, [[(10, 10), (10, 12), (12, 12), (12, 10), (10, 10)]]);
WITH
    groupPolygonUnion(polygon) AS actual,
    multiIf(
        grp = 1,
        readWKTMultiPolygon('MULTIPOLYGON(((1 2,1 3,3 3,3 1,2 1,2 0,0 0,0 2,1 2)))'),
        grp = 2,
        readWKTMultiPolygon('MULTIPOLYGON(((10 10,10 12,12 12,12 10,10 10)))'),
        readWKTMultiPolygon('MULTIPOLYGON()')
    ) AS expected
SELECT grp, polygonsEqualsCartesian(actual, expected)
FROM geo
GROUP BY grp
ORDER BY grp;
DROP TABLE geo;

-- Disjoint polygons remain separate in the result
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]]);
INSERT INTO geo VALUES ([[(10, 10), (10, 11), (11, 11), (11, 10), (10, 10)]]);
WITH
    groupPolygonUnion(polygon) AS actual,
    readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)),((10 10,10 11,11 11,11 10,10 10)))') AS expected
SELECT 'Disjoint:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Test correct_geometry parameter (second argument = 1)
CREATE TABLE geo (polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], 1);
INSERT INTO geo VALUES ([[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]], 1);
WITH
    groupPolygonUnion(polygon, should_correct) AS actual,
    readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))') AS expected
SELECT 'correct_geometry=1:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Test correct_geometry parameter with all geometries having same correction setting
CREATE TABLE geo (polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], 0);
INSERT INTO geo VALUES ([[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]], 0);
WITH
    groupPolygonUnion(polygon, should_correct) AS actual,
    readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))') AS expected
SELECT 'correct_geometry=0:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Test correct_geometry parameter with mixed values
CREATE TABLE geo (polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], 1);
INSERT INTO geo VALUES ([[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]], 0);
WITH
    groupPolygonUnion(polygon, should_correct) AS actual,
    readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))') AS expected
SELECT 'correct_geometry mixed:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;


-- GROUP BY with correct_geometry parameter
CREATE TABLE geo (grp Int32, polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES (1, [[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], 1);
INSERT INTO geo VALUES (1, [[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]], 1);
INSERT INTO geo VALUES (2, [[(10, 10), (10, 15), (15, 15), (15, 10), (10, 10)]], 0);
INSERT INTO geo VALUES (2, [[(12, 12), (12, 18), (18, 18), (18, 12), (12, 12)]], 0);
WITH
    groupPolygonUnion(polygon, should_correct) AS actual,
    multiIf(
        grp = 1,
        readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))'),
        grp = 2,
        readWKTMultiPolygon('MULTIPOLYGON(((12 15,12 18,18 18,18 12,15 12,15 10,10 10,10 15,12 15)))'),
        readWKTMultiPolygon('MULTIPOLYGON()')
    ) AS expected
SELECT grp, polygonsEqualsCartesian(actual, expected)
FROM geo
GROUP BY grp
ORDER BY grp;
DROP TABLE geo;

-- Test with Ring input and correct_geometry
CREATE TABLE geo (ring Ring, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)], 1);
INSERT INTO geo VALUES ([(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)], 1);
WITH
    groupPolygonUnion(ring, should_correct) AS actual,
    readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))') AS expected
SELECT 'Ring with correct_geometry:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Test with MultiPolygon input and correct_geometry
CREATE TABLE geo (mpoly MultiPolygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]], 1);
INSERT INTO geo VALUES ([[[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]]], 1);
WITH
    groupPolygonUnion(mpoly, should_correct) AS actual,
    readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))') AS expected
SELECT 'MultiPolygon with correct_geometry:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Geometry (Variant) input: mixed Ring and Polygon
CREATE TABLE geo (g Geometry) ENGINE = Memory();
INSERT INTO geo VALUES ([(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]::Ring::Geometry);
INSERT INTO geo VALUES ([[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]]::Polygon::Geometry);
WITH
    groupPolygonUnion(g) AS actual,
    readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))') AS expected
SELECT 'Geometry Ring+Polygon union:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Geometry (Variant) input: mixed Polygon and MultiPolygon
CREATE TABLE geo (g Geometry) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]::Polygon::Geometry);
INSERT INTO geo VALUES ([[[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]]]::MultiPolygon::Geometry);
WITH
    groupPolygonUnion(g) AS actual,
    readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))') AS expected
SELECT 'Geometry Polygon+MultiPolygon union:', polygonsEqualsCartesian(actual, expected) FROM geo;
DROP TABLE geo;

-- Geometry (Variant) input: unsupported Point variant should fail
SELECT groupPolygonUnion(g) FROM (SELECT (0.,0.)::Point::Geometry AS g); -- { serverError BAD_ARGUMENTS }
