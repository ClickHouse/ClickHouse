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
SELECT 'Point single:', wkt(groupConvexHull(p)) FROM geo;
INSERT INTO geo VALUES ((10, 0));
INSERT INTO geo VALUES ((10, 10));
INSERT INTO geo VALUES ((0, 10));
INSERT INTO geo VALUES ((5, 5));
SELECT 'Point five (square + interior):', wkt(groupConvexHull(p)) FROM geo;
DROP TABLE geo;

-- Ring input tests
CREATE TABLE geo (ring Ring) ENGINE = Memory();
SELECT 'Ring empty:', wkt(groupConvexHull(ring)) FROM geo;
INSERT INTO geo VALUES ([(0, 0), (5, 0), (5, 5), (0, 5), (0, 0)]);
SELECT 'Ring single:', wkt(groupConvexHull(ring)) FROM geo;
INSERT INTO geo VALUES ([(10, 10), (20, 10), (20, 20), (10, 20), (10, 10)]);
SELECT 'Ring two disjoint:', wkt(groupConvexHull(ring)) FROM geo;
DROP TABLE geo;

-- Polygon input tests (holes should be ignored for convex hull)
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
SELECT 'Polygon empty:', wkt(groupConvexHull(polygon)) FROM geo;
INSERT INTO geo VALUES ([[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)], [(2, 2), (3, 2), (3, 3), (2, 3), (2, 2)]]);
SELECT 'Polygon single with hole:', wkt(groupConvexHull(polygon)) FROM geo;
INSERT INTO geo VALUES ([[(20, 20), (20, 30), (30, 30), (30, 20), (20, 20)]]);
SELECT 'Polygon two disjoint:', wkt(groupConvexHull(polygon)) FROM geo;
DROP TABLE geo;

-- MultiPolygon input tests
CREATE TABLE geo (mpoly MultiPolygon) ENGINE = Memory();
SELECT 'MultiPolygon empty:', wkt(groupConvexHull(mpoly)) FROM geo;
INSERT INTO geo VALUES ([[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], [[(10, 10), (10, 15), (15, 15), (15, 10), (10, 10)]]]);
SELECT 'MultiPolygon single (two polys):', wkt(groupConvexHull(mpoly)) FROM geo;
INSERT INTO geo VALUES ([[[(20, 0), (20, 5), (25, 5), (25, 0), (20, 0)]]]);
SELECT 'MultiPolygon two rows:', wkt(groupConvexHull(mpoly)) FROM geo;
DROP TABLE geo;

-- LineString input tests
SELECT 'LineString two rows:', wkt(groupConvexHull(ls))
FROM (
    SELECT readWKTLineString(s) AS ls FROM (
        SELECT arrayJoin(['LINESTRING (0 0, 10 0, 10 10)', 'LINESTRING (0 10, 5 15, 10 10)']) AS s
    )
);

-- MultiLineString input tests
SELECT 'MultiLineString two rows:', wkt(groupConvexHull(mls))
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
SELECT grp, wkt(groupConvexHull(p)) FROM geo GROUP BY grp ORDER BY grp;
DROP TABLE geo;

-- Collinear points (degenerate hull)
CREATE TABLE geo (p Point) ENGINE = Memory();
INSERT INTO geo VALUES ((0, 0));
INSERT INTO geo VALUES ((5, 5));
INSERT INTO geo VALUES ((10, 10));
SELECT 'Collinear:', wkt(groupConvexHull(p)) FROM geo;
DROP TABLE geo;

-- Two identical points
CREATE TABLE geo (p Point) ENGINE = Memory();
INSERT INTO geo VALUES ((3, 4));
INSERT INTO geo VALUES ((3, 4));
SELECT 'Identical points:', wkt(groupConvexHull(p)) FROM geo;
DROP TABLE geo;

-- Single point
CREATE TABLE geo (p Point) ENGINE = Memory();
INSERT INTO geo VALUES ((7, 3));
SELECT 'Single point:', wkt(groupConvexHull(p)) FROM geo;
DROP TABLE geo;

-- Triangle from points
CREATE TABLE geo (p Point) ENGINE = Memory();
INSERT INTO geo VALUES ((0, 0));
INSERT INTO geo VALUES ((10, 0));
INSERT INTO geo VALUES ((5, 10));
SELECT 'Triangle:', wkt(groupConvexHull(p)) FROM geo;
DROP TABLE geo;

-- Test correct_geometry parameter with Point input
CREATE TABLE geo (p Point, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ((0, 0), 1);
INSERT INTO geo VALUES ((10, 0), 1);
INSERT INTO geo VALUES ((10, 10), 1);
INSERT INTO geo VALUES ((0, 10), 1);
SELECT 'Point correct=1:', wkt(groupConvexHull(p, should_correct)) FROM geo;
DROP TABLE geo;

CREATE TABLE geo (p Point, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ((0, 0), 0);
INSERT INTO geo VALUES ((10, 0), 0);
INSERT INTO geo VALUES ((10, 10), 0);
INSERT INTO geo VALUES ((0, 10), 0);
SELECT 'Point correct=0:', wkt(groupConvexHull(p, should_correct)) FROM geo;
DROP TABLE geo;

-- Test correct_geometry with Ring input
CREATE TABLE geo (ring Ring, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([(0, 0), (5, 0), (5, 5), (0, 5), (0, 0)], 1);
INSERT INTO geo VALUES ([(10, 10), (20, 10), (20, 20), (10, 20), (10, 10)], 1);
SELECT 'Ring correct=1:', wkt(groupConvexHull(ring, should_correct)) FROM geo;
DROP TABLE geo;

-- Test correct_geometry with Polygon input
CREATE TABLE geo (polygon Polygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]], 1);
INSERT INTO geo VALUES ([[(20, 20), (20, 30), (30, 30), (30, 20), (20, 20)]], 0);
SELECT 'Polygon correct mixed:', wkt(groupConvexHull(polygon, should_correct)) FROM geo;
DROP TABLE geo;

-- Test correct_geometry with MultiPolygon input
CREATE TABLE geo (mpoly MultiPolygon, should_correct UInt8) ENGINE = Memory();
INSERT INTO geo VALUES ([[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]], 1);
INSERT INTO geo VALUES ([[[(10, 10), (10, 15), (15, 15), (15, 10), (10, 10)]]], 1);
SELECT 'MultiPolygon correct=1:', wkt(groupConvexHull(mpoly, should_correct)) FROM geo;
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
SELECT grp, wkt(groupConvexHull(polygon, should_correct)) FROM geo GROUP BY grp ORDER BY grp;
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
SELECT 'Pentagon with interior:', wkt(groupConvexHull(p)) FROM geo;
DROP TABLE geo;

-- Polygon with hole: convex hull should cover the outer ring only
CREATE TABLE geo (polygon Polygon) ENGINE = Memory();
INSERT INTO geo VALUES ([[(0, 0), (0, 20), (20, 20), (20, 0), (0, 0)], [(5, 5), (5, 15), (15, 15), (15, 5), (5, 5)]]);
SELECT 'Polygon hole ignored:', wkt(groupConvexHull(polygon)) FROM geo;
DROP TABLE geo;

-- GROUP BY with Ring input
CREATE TABLE geo (grp Int32, ring Ring) ENGINE = Memory();
INSERT INTO geo VALUES (1, [(0, 0), (5, 0), (5, 5), (0, 5), (0, 0)]);
INSERT INTO geo VALUES (1, [(3, 3), (8, 3), (8, 8), (3, 8), (3, 3)]);
INSERT INTO geo VALUES (2, [(10, 10), (15, 10), (15, 15), (10, 15), (10, 10)]);
SELECT grp, wkt(groupConvexHull(ring)) FROM geo GROUP BY grp ORDER BY grp;
DROP TABLE geo;
