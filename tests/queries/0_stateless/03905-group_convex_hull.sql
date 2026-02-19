-- Tags: no-fasttest

-- Test groupConvexHull with Point, Ring, Polygon, MultiPolygon, LineString, MultiLineString inputs

DROP TABLE IF EXISTS geo;

-- Negative tests: wrong argument type, invalid types
SELECT groupConvexHull(42); -- { serverError BAD_ARGUMENTS }
SELECT groupConvexHull('not a polygon'); -- { serverError BAD_ARGUMENTS }
SELECT groupConvexHull(p, p, p) FROM (SELECT (0.,0.)::Point AS p); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Invalid correct_geometry type
SELECT groupConvexHull(p, p) FROM (SELECT (0.,0.)::Point AS p); -- { serverError BAD_ARGUMENTS }
SELECT groupConvexHull(p, 1.5) FROM (SELECT (0.,0.)::Point AS p); -- { serverError BAD_ARGUMENTS }

-- Point input tests
CREATE TABLE geo (p Point) ENGINE = Memory();
SELECT 'Point empty:', wkt(groupConvexHull(p)) FROM geo;
INSERT INTO geo VALUES ((0, 0));
WITH
    groupConvexHull(p) AS actual,
    readWKTPolygon('POLYGON((0 0,0 0,0 0,0 0))') AS expected
SELECT 'Point single:', polygonsEqualsCartesian([actual], expected) FROM geo;
INSERT INTO geo VALUES ((10, 0));
INSERT INTO geo VALUES ((10, 10));
INSERT INTO geo VALUES ((0, 10));
INSERT INTO geo VALUES ((5, 5));
WITH
    groupConvexHull(p) AS actual,
    readWKTPolygon('POLYGON((0 0,0 10,10 10,10 0,0 0))') AS expected
SELECT 'Point five (square + interior):', polygonsEqualsCartesian([actual], expected) FROM geo;
DROP TABLE geo;

-- Ring input tests
CREATE TABLE geo (ring Ring) ENGINE = Memory();
SELECT 'Ring empty:', wkt(groupConvexHull(ring)) FROM geo;
INSERT INTO geo VALUES ([(0, 0), (5, 0), (5, 5), (0, 5), (0, 0)]);
WITH
    groupConvexHull(ring) AS actual,
    readWKTPolygon('POLYGON((0 0,0 5,5 5,5 0,0 0))') AS expected
SELECT 'Ring single:', polygonsEqualsCartesian([actual], expected) FROM geo;
INSERT INTO geo VALUES ([(10, 10), (20, 10), (20, 20), (10, 20), (10, 10)]);
WITH
    groupConvexHull(ring) AS actual,
    readWKTPolygon('POLYGON((0 0,0 5,10 20,20 20,20 10,5 0,0 0))') AS expected
SELECT 'Ring two disjoint:', polygonsEqualsCartesian([actual], expected) FROM geo;
DROP TABLE geo;

-- Polygon input tests (holes should be ignored for convex hull)
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
SELECT 'Polygon empty:', wkt(groupConvexHull(polygon)) FROM geo;
INSERT INTO geo VALUES ([[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)], [(2, 2), (3, 2), (3, 3), (2, 3), (2, 2)]]);
WITH
    groupConvexHull(polygon) AS actual,
    readWKTPolygon('POLYGON((0 0,0 10,10 10,10 0,0 0))') AS expected
SELECT 'Polygon single with hole:', polygonsEqualsCartesian([actual], expected) FROM geo;
INSERT INTO geo VALUES ([[(20, 20), (20, 30), (30, 30), (30, 20), (20, 20)]]);
WITH
    groupConvexHull(polygon) AS actual,
    readWKTPolygon('POLYGON((0 0,0 10,20 30,30 30,30 20,10 0,0 0))') AS expected
SELECT 'Polygon two disjoint:', polygonsEqualsCartesian([actual], expected) FROM geo;
DROP TABLE geo;

-- MultiPolygon input tests
CREATE TABLE geo (mpoly MultiPolygon) ENGINE = Memory();
SELECT 'MultiPolygon empty:', wkt(groupConvexHull(mpoly)) FROM geo;
INSERT INTO geo VALUES ([[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], [[(10, 10), (10, 15), (15, 15), (15, 10), (10, 10)]]]);
WITH
    groupConvexHull(mpoly) AS actual,
    readWKTPolygon('POLYGON((0 0,0 5,10 15,15 15,15 10,5 0,0 0))') AS expected
SELECT 'MultiPolygon single (two polys):', polygonsEqualsCartesian([actual], expected) FROM geo;
INSERT INTO geo VALUES ([[[(20, 0), (20, 5), (25, 5), (25, 0), (20, 0)]]]);
WITH
    groupConvexHull(mpoly) AS actual,
    readWKTPolygon('POLYGON((0 0,0 5,10 15,15 15,25 5,25 0,0 0))') AS expected
SELECT 'MultiPolygon two rows:', polygonsEqualsCartesian([actual], expected) FROM geo;
DROP TABLE geo;

-- LineString input tests
WITH readWKTPolygon('POLYGON((0 0,0 10,5 15,10 10,10 0,0 0))') AS expected
SELECT 'LineString two rows:', polygonsEqualsCartesian([groupConvexHull(ls)], expected)
FROM (
    SELECT readWKTLineString(s) AS ls FROM (
        SELECT arrayJoin(['LINESTRING (0 0, 10 0, 10 10)', 'LINESTRING (0 10, 5 15, 10 10)']) AS s
    )
);

-- MultiLineString input tests
WITH readWKTPolygon('POLYGON((0 0,5 15,10 0,5 -5,0 0))') AS expected
SELECT 'MultiLineString two rows:', polygonsEqualsCartesian([groupConvexHull(mls)], expected)
FROM (
    SELECT readWKTMultiLineString(s) AS mls FROM (
        SELECT arrayJoin([
            'MULTILINESTRING ((0 0, 10 0), (0 10, 10 10))',
            'MULTILINESTRING ((5 -5, 5 15))'
        ]) AS s
    )
);

-- Return type is always Ring regardless of input type
SELECT 'Point type:', toTypeName(groupConvexHull(p)),
       'Ring type:', toTypeName(groupConvexHull(r)),
       'Polygon type:', toTypeName(groupConvexHull(pg)),
       'MultiPolygon type:', toTypeName(groupConvexHull(mp))
FROM (
    SELECT (0.,0.)::Point AS p,
           [(0.,0.),(1.,0.),(1.,1.),(0.,0.)]::Ring AS r,
           [[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]]::Polygon AS pg,
           [[[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]]]::MultiPolygon AS mp
);

-- GROUP BY test
CREATE TABLE geo (grp Int32, p Point) ENGINE = Memory();
INSERT INTO geo VALUES (1, (0, 0));
INSERT INTO geo VALUES (1, (10, 0));
INSERT INTO geo VALUES (1, (10, 10));
INSERT INTO geo VALUES (1, (0, 10));
INSERT INTO geo VALUES (2, (100, 100));
INSERT INTO geo VALUES (2, (110, 100));
INSERT INTO geo VALUES (2, (110, 110));
INSERT INTO geo VALUES (2, (100, 110));
INSERT INTO geo VALUES (2, (105, 105));
WITH
    groupConvexHull(p) AS actual,
    multiIf(
        grp = 1,
        readWKTPolygon('POLYGON((0 0,0 10,10 10,10 0,0 0))'),
        grp = 2,
        readWKTPolygon('POLYGON((100 100,100 110,110 110,110 100,100 100))'),
        readWKTPolygon('POLYGON()')
    ) AS expected
SELECT grp, polygonsEqualsCartesian([actual], expected) FROM geo GROUP BY grp ORDER BY grp;
DROP TABLE geo;

-- Collinear points (degenerate hull)
CREATE TABLE geo (p Point) ENGINE = Memory();
INSERT INTO geo VALUES ((0, 0));
INSERT INTO geo VALUES ((5, 5));
INSERT INTO geo VALUES ((10, 10));
WITH
    groupConvexHull(p) AS actual,
    readWKTPolygon('POLYGON((0 0,10 10,0 0,0 0))') AS expected
SELECT 'Collinear:', polygonsEqualsCartesian([actual], expected) FROM geo;
DROP TABLE geo;

-- Two identical points
CREATE TABLE geo (p Point) ENGINE = Memory();
INSERT INTO geo VALUES ((3, 4));
INSERT INTO geo VALUES ((3, 4));
WITH
    groupConvexHull(p) AS actual,
    readWKTPolygon('POLYGON((3 4,3 4,3 4,3 4))') AS expected
SELECT 'Identical points:', polygonsEqualsCartesian([actual], expected) FROM geo;
DROP TABLE geo;

-- Single point
CREATE TABLE geo (p Point) ENGINE = Memory();
INSERT INTO geo VALUES ((7, 3));
WITH
    groupConvexHull(p) AS actual,
    readWKTPolygon('POLYGON((7 3,7 3,7 3,7 3))') AS expected
SELECT 'Single point:', polygonsEqualsCartesian([actual], expected) FROM geo;
DROP TABLE geo;

-- Triangle from points
CREATE TABLE geo (p Point) ENGINE = Memory();
INSERT INTO geo VALUES ((0, 0));
INSERT INTO geo VALUES ((10, 0));
INSERT INTO geo VALUES ((5, 10));
WITH
    groupConvexHull(p) AS actual,
    readWKTPolygon('POLYGON((0 0,5 10,10 0,0 0))') AS expected
SELECT 'Triangle:', polygonsEqualsCartesian([actual], expected) FROM geo;
DROP TABLE geo;

-- Test correct_geometry parameter with Point input
CREATE TABLE geo (p Point, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ((0, 0), 1);
INSERT INTO geo VALUES ((10, 0), 1);
INSERT INTO geo VALUES ((10, 10), 1);
INSERT INTO geo VALUES ((0, 10), 1);
WITH
    groupConvexHull(p, should_correct) AS actual,
    readWKTPolygon('POLYGON((0 0,0 10,10 10,10 0,0 0))') AS expected
SELECT 'Point correct=1:', polygonsEqualsCartesian([actual], expected) FROM geo;
DROP TABLE geo;

CREATE TABLE geo (p Point, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ((0, 0), 0);
INSERT INTO geo VALUES ((10, 0), 0);
INSERT INTO geo VALUES ((10, 10), 0);
INSERT INTO geo VALUES ((0, 10), 0);
WITH
    groupConvexHull(p, should_correct) AS actual,
    readWKTPolygon('POLYGON((0 0,0 10,10 10,10 0,0 0))') AS expected
SELECT 'Point correct=0:', polygonsEqualsCartesian([actual], expected) FROM geo;
DROP TABLE geo;

-- Test correct_geometry with Ring input
CREATE TABLE geo (ring Ring, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([(0, 0), (5, 0), (5, 5), (0, 5), (0, 0)], 1);
INSERT INTO geo VALUES ([(10, 10), (20, 10), (20, 20), (10, 20), (10, 10)], 1);
WITH
    groupConvexHull(ring, should_correct) AS actual,
    readWKTPolygon('POLYGON((0 0,0 5,10 20,20 20,20 10,5 0,0 0))') AS expected
SELECT 'Ring correct=1:', polygonsEqualsCartesian([actual], expected) FROM geo;
DROP TABLE geo;

-- Test correct_geometry with Polygon input
CREATE TABLE geo (polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]], 1);
INSERT INTO geo VALUES ([[(20, 20), (20, 30), (30, 30), (30, 20), (20, 20)]], 0);
WITH
    groupConvexHull(polygon, should_correct) AS actual,
    readWKTPolygon('POLYGON((0 0,0 10,20 30,30 30,30 20,10 0,0 0))') AS expected
SELECT 'Polygon correct mixed:', polygonsEqualsCartesian([actual], expected) FROM geo;
DROP TABLE geo;

-- Test correct_geometry with MultiPolygon input
CREATE TABLE geo (mpoly MultiPolygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]], 1);
INSERT INTO geo VALUES ([[[(10, 10), (10, 15), (15, 15), (15, 10), (10, 10)]]], 1);
WITH
    groupConvexHull(mpoly, should_correct) AS actual,
    readWKTPolygon('POLYGON((0 0,0 5,10 15,15 15,15 10,5 0,0 0))') AS expected
SELECT 'MultiPolygon correct=1:', polygonsEqualsCartesian([actual], expected) FROM geo;
DROP TABLE geo;

-- Empty table with correct_geometry parameter
CREATE TABLE geo (p Point, should_correct UInt8) ENGINE = Memory();
SELECT 'Empty with correct_geometry:', wkt(groupConvexHull(p, should_correct)) FROM geo;
DROP TABLE geo;

-- GROUP BY with correct_geometry parameter
CREATE TABLE geo (grp Int32, polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES (1, [[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]], 1);
INSERT INTO geo VALUES (1, [[(5, 5), (5, 15), (15, 15), (15, 5), (5, 5)]], 1);
INSERT INTO geo VALUES (2, [[(100, 100), (100, 110), (110, 110), (110, 100), (100, 100)]], 0);
INSERT INTO geo VALUES (2, [[(105, 105), (105, 115), (115, 115), (115, 105), (105, 105)]], 0);
WITH
    groupConvexHull(polygon, should_correct) AS actual,
    multiIf(
        grp = 1,
        readWKTPolygon('POLYGON((0 0,0 10,5 15,15 15,15 5,10 0,0 0))'),
        grp = 2,
        readWKTPolygon('POLYGON((100 100,100 110,105 115,115 115,115 105,110 100,100 100))'),
        readWKTPolygon('POLYGON()')
    ) AS expected
SELECT grp, polygonsEqualsCartesian([actual], expected) FROM geo GROUP BY grp ORDER BY grp;
DROP TABLE geo;

-- Many points forming a known convex shape (pentagon-like)
CREATE TABLE geo (p Point) ENGINE = Memory();
INSERT INTO geo VALUES ((5, 0));
INSERT INTO geo VALUES ((10, 4));
INSERT INTO geo VALUES ((8, 10));
INSERT INTO geo VALUES ((2, 10));
INSERT INTO geo VALUES ((0, 4));
INSERT INTO geo VALUES ((5, 5));
INSERT INTO geo VALUES ((5, 3));
INSERT INTO geo VALUES ((4, 6));
WITH
    groupConvexHull(p) AS actual,
    readWKTPolygon('POLYGON((0 4,2 10,8 10,10 4,5 0,0 4))') AS expected
SELECT 'Pentagon with interior:', polygonsEqualsCartesian([actual], expected) FROM geo;
DROP TABLE geo;

-- Polygon with hole: convex hull should cover the outer ring only
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 20), (20, 20), (20, 0), (0, 0)], [(5, 5), (5, 15), (15, 15), (15, 5), (5, 5)]]);
WITH
    groupConvexHull(polygon) AS actual,
    readWKTPolygon('POLYGON((0 0,0 20,20 20,20 0,0 0))') AS expected
SELECT 'Polygon hole ignored:', polygonsEqualsCartesian([actual], expected) FROM geo;
DROP TABLE geo;

-- GROUP BY with Ring input
CREATE TABLE geo (grp Int32, ring Ring) ENGINE = Memory();
INSERT INTO geo VALUES (1, [(0, 0), (5, 0), (5, 5), (0, 5), (0, 0)]);
INSERT INTO geo VALUES (1, [(3, 3), (8, 3), (8, 8), (3, 8), (3, 3)]);
INSERT INTO geo VALUES (2, [(10, 10), (15, 10), (15, 15), (10, 15), (10, 10)]);
WITH
    groupConvexHull(ring) AS actual,
    multiIf(
        grp = 1,
        readWKTPolygon('POLYGON((0 0,0 5,3 8,8 8,8 3,5 0,0 0))'),
        grp = 2,
        readWKTPolygon('POLYGON((10 10,10 15,15 15,15 10,10 10))'),
        readWKTPolygon('POLYGON()')
    ) AS expected
SELECT grp, polygonsEqualsCartesian([actual], expected) FROM geo GROUP BY grp ORDER BY grp;
DROP TABLE geo;
