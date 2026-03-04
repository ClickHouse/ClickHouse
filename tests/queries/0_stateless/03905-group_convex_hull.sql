-- Tags: no-fasttest

-- Test groupConvexHull with Point, Ring, Polygon, MultiPolygon, LineString, MultiLineString, and Geometry inputs

-- =============================================================================
-- Negative tests
-- =============================================================================

SELECT groupConvexHull(42); -- { serverError BAD_ARGUMENTS }
SELECT groupConvexHull('not a polygon'); -- { serverError BAD_ARGUMENTS }
SELECT groupConvexHull(p, p, p)
    FROM (SELECT (0.,0.)::Point AS p); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Invalid correct_geometry type
SELECT groupConvexHull(p, p)
    FROM (SELECT (0.,0.)::Point AS p); -- { serverError BAD_ARGUMENTS }
SELECT groupConvexHull(p, 1.5)
    FROM (SELECT (0.,0.)::Point AS p); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- Point input
-- =============================================================================

DROP TABLE IF EXISTS geo;
CREATE TABLE geo (p Point) ENGINE = Memory;

SELECT 'Point empty:', wkt(groupConvexHull(p)) FROM geo;

INSERT INTO geo VALUES ((0, 0));
SELECT 'Point single:',
    polygonsEqualsCartesian(
        [groupConvexHull(p)],
        readWKTPolygon('POLYGON((0 0,0 0,0 0,0 0))'))
FROM geo;

INSERT INTO geo VALUES ((10, 0)), ((10, 10)), ((0, 10)), ((5, 5));
SELECT 'Point five (square + interior):',
    polygonsEqualsCartesian(
        [groupConvexHull(p)],
        readWKTPolygon('POLYGON((0 0,0 10,10 10,10 0,0 0))'))
FROM geo;

DROP TABLE geo;

-- =============================================================================
-- Ring input
-- =============================================================================

CREATE TABLE geo (ring Ring) ENGINE = Memory;

SELECT 'Ring empty:', wkt(groupConvexHull(ring)) FROM geo;

INSERT INTO geo VALUES ([(0, 0), (5, 0), (5, 5), (0, 5), (0, 0)]);
SELECT 'Ring single:',
    polygonsEqualsCartesian(
        [groupConvexHull(ring)],
        readWKTPolygon('POLYGON((0 0,0 5,5 5,5 0,0 0))'))
FROM geo;

INSERT INTO geo VALUES ([(10, 10), (20, 10), (20, 20), (10, 20), (10, 10)]);
SELECT 'Ring two disjoint:',
    polygonsEqualsCartesian(
        [groupConvexHull(ring)],
        readWKTPolygon('POLYGON((0 0,0 5,10 20,20 20,20 10,5 0,0 0))'))
FROM geo;

DROP TABLE geo;

-- =============================================================================
-- Polygon input (holes are ignored for convex hull)
-- =============================================================================

CREATE TABLE geo (polygon Polygon) ENGINE = Memory;

SELECT 'Polygon empty:', wkt(groupConvexHull(polygon)) FROM geo;

INSERT INTO geo VALUES ([[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)], [(2, 2), (3, 2), (3, 3), (2, 3), (2, 2)]]);
SELECT 'Polygon single with hole:',
    polygonsEqualsCartesian(
        [groupConvexHull(polygon)],
        readWKTPolygon('POLYGON((0 0,0 10,10 10,10 0,0 0))'))
FROM geo;

INSERT INTO geo VALUES ([[(20, 20), (20, 30), (30, 30), (30, 20), (20, 20)]]);
SELECT 'Polygon two disjoint:',
    polygonsEqualsCartesian(
        [groupConvexHull(polygon)],
        readWKTPolygon('POLYGON((0 0,0 10,20 30,30 30,30 20,10 0,0 0))'))
FROM geo;

DROP TABLE geo;

-- =============================================================================
-- MultiPolygon input
-- =============================================================================

CREATE TABLE geo (mpoly MultiPolygon) ENGINE = Memory;

SELECT 'MultiPolygon empty:', wkt(groupConvexHull(mpoly)) FROM geo;

INSERT INTO geo VALUES ([[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], [[(10, 10), (10, 15), (15, 15), (15, 10), (10, 10)]]]);
SELECT 'MultiPolygon single (two polys):',
    polygonsEqualsCartesian(
        [groupConvexHull(mpoly)],
        readWKTPolygon('POLYGON((0 0,0 5,10 15,15 15,15 10,5 0,0 0))'))
FROM geo;

INSERT INTO geo VALUES ([[[(20, 0), (20, 5), (25, 5), (25, 0), (20, 0)]]]);
SELECT 'MultiPolygon two rows:',
    polygonsEqualsCartesian(
        [groupConvexHull(mpoly)],
        readWKTPolygon('POLYGON((0 0,0 5,10 15,15 15,25 5,25 0,0 0))'))
FROM geo;

DROP TABLE geo;

-- =============================================================================
-- LineString input
-- =============================================================================

CREATE TABLE geo (ls LineString) ENGINE = Memory;
INSERT INTO geo SELECT readWKTLineString(s) FROM (
    SELECT arrayJoin([
        'LINESTRING (0 0, 10 0, 10 10)',
        'LINESTRING (0 10, 5 15, 10 10)'
    ]) AS s
);
SELECT 'LineString two rows:',
    polygonsEqualsCartesian(
        [groupConvexHull(ls)],
        readWKTPolygon('POLYGON((0 0,0 10,5 15,10 10,10 0,0 0))'))
FROM geo;
DROP TABLE geo;

-- =============================================================================
-- MultiLineString input
-- =============================================================================

CREATE TABLE geo (mls MultiLineString) ENGINE = Memory;
INSERT INTO geo SELECT readWKTMultiLineString(s) FROM (
    SELECT arrayJoin([
        'MULTILINESTRING ((0 0, 10 0), (0 10, 10 10))',
        'MULTILINESTRING ((5 -5, 5 15))'
    ]) AS s
);
SELECT 'MultiLineString two rows:',
    polygonsEqualsCartesian(
        [groupConvexHull(mls)],
        readWKTPolygon('POLYGON((0 0,5 15,10 0,5 -5,0 0))'))
FROM geo;
DROP TABLE geo;

-- =============================================================================
-- Return type is always Ring
-- =============================================================================

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

-- =============================================================================
-- GROUP BY
-- =============================================================================

CREATE TABLE geo (grp Int32, p Point) ENGINE = Memory;
INSERT INTO geo VALUES
    (1, (0, 0)), (1, (10, 0)), (1, (10, 10)), (1, (0, 10)),
    (2, (100, 100)), (2, (110, 100)), (2, (110, 110)), (2, (100, 110)), (2, (105, 105));

SELECT grp,
    polygonsEqualsCartesian(
        [groupConvexHull(p)],
        if(grp = 1,
            readWKTPolygon('POLYGON((0 0,0 10,10 10,10 0,0 0))'),
            readWKTPolygon('POLYGON((100 100,100 110,110 110,110 100,100 100))')))
FROM geo
GROUP BY grp
ORDER BY grp;

DROP TABLE geo;

-- =============================================================================
-- Degenerate cases
-- =============================================================================

CREATE TABLE geo (p Point) ENGINE = Memory;

-- Collinear points
TRUNCATE TABLE geo;
INSERT INTO geo VALUES ((0, 0)), ((5, 5)), ((10, 10));
SELECT 'Collinear:',
    polygonsEqualsCartesian(
        [groupConvexHull(p)],
        readWKTPolygon('POLYGON((0 0,10 10,0 0,0 0))'))
FROM geo;

-- Two identical points
TRUNCATE TABLE geo;
INSERT INTO geo VALUES ((3, 4)), ((3, 4));
SELECT 'Identical points:',
    polygonsEqualsCartesian(
        [groupConvexHull(p)],
        readWKTPolygon('POLYGON((3 4,3 4,3 4,3 4))'))
FROM geo;

-- Single point
TRUNCATE TABLE geo;
INSERT INTO geo VALUES ((7, 3));
SELECT 'Single point:',
    polygonsEqualsCartesian(
        [groupConvexHull(p)],
        readWKTPolygon('POLYGON((7 3,7 3,7 3,7 3))'))
FROM geo;

-- Triangle from points
TRUNCATE TABLE geo;
INSERT INTO geo VALUES ((0, 0)), ((10, 0)), ((5, 10));
SELECT 'Triangle:',
    polygonsEqualsCartesian(
        [groupConvexHull(p)],
        readWKTPolygon('POLYGON((0 0,5 10,10 0,0 0))'))
FROM geo;

DROP TABLE geo;

-- =============================================================================
-- correct_geometry parameter
-- =============================================================================

-- Point with correct=1
CREATE TABLE geo (p Point) ENGINE = Memory;
INSERT INTO geo VALUES ((0, 0)), ((10, 0)), ((10, 10)), ((0, 10));
SELECT 'Point correct=1:',
    polygonsEqualsCartesian(
        [groupConvexHull(p, toUInt8(1))],
        readWKTPolygon('POLYGON((0 0,0 10,10 10,10 0,0 0))'))
FROM geo;
DROP TABLE geo;

-- Point with correct=0
CREATE TABLE geo (p Point) ENGINE = Memory;
INSERT INTO geo VALUES ((0, 0)), ((10, 0)), ((10, 10)), ((0, 10));
SELECT 'Point correct=0:',
    polygonsEqualsCartesian(
        [groupConvexHull(p, toUInt8(0))],
        readWKTPolygon('POLYGON((0 0,0 10,10 10,10 0,0 0))'))
FROM geo;
DROP TABLE geo;

-- Ring with correct=1
CREATE TABLE geo (ring Ring, should_correct UInt8) ENGINE = Memory;
INSERT INTO geo VALUES
    ([(0, 0), (5, 0), (5, 5), (0, 5), (0, 0)], 1),
    ([(10, 10), (20, 10), (20, 20), (10, 20), (10, 10)], 1);
SELECT 'Ring correct=1:',
    polygonsEqualsCartesian(
        [groupConvexHull(ring, should_correct)],
        readWKTPolygon('POLYGON((0 0,0 5,10 20,20 20,20 10,5 0,0 0))'))
FROM geo;
DROP TABLE geo;

-- Polygon with mixed correct_geometry
CREATE TABLE geo (polygon Polygon, should_correct UInt8) ENGINE = Memory;
INSERT INTO geo VALUES
    ([[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]], 1),
    ([[(20, 20), (20, 30), (30, 30), (30, 20), (20, 20)]], 0);
SELECT 'Polygon correct mixed:',
    polygonsEqualsCartesian(
        [groupConvexHull(polygon, should_correct)],
        readWKTPolygon('POLYGON((0 0,0 10,20 30,30 30,30 20,10 0,0 0))'))
FROM geo;
DROP TABLE geo;

-- MultiPolygon with correct=1
CREATE TABLE geo (mpoly MultiPolygon, should_correct UInt8) ENGINE = Memory;
INSERT INTO geo VALUES
    ([[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]], 1),
    ([[[(10, 10), (10, 15), (15, 15), (15, 10), (10, 10)]]], 1);
SELECT 'MultiPolygon correct=1:',
    polygonsEqualsCartesian(
        [groupConvexHull(mpoly, should_correct)],
        readWKTPolygon('POLYGON((0 0,0 5,10 15,15 15,15 10,5 0,0 0))'))
FROM geo;
DROP TABLE geo;

-- Empty table with correct_geometry
CREATE TABLE geo (p Point, should_correct UInt8) ENGINE = Memory;
SELECT 'Empty with correct_geometry:', wkt(groupConvexHull(p, should_correct)) FROM geo;
DROP TABLE geo;

-- GROUP BY with correct_geometry
CREATE TABLE geo (grp Int32, polygon Polygon, should_correct UInt8) ENGINE = Memory;
INSERT INTO geo VALUES
    (1, [[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]], 1),
    (1, [[(5, 5), (5, 15), (15, 15), (15, 5), (5, 5)]], 1),
    (2, [[(100, 100), (100, 110), (110, 110), (110, 100), (100, 100)]], 0),
    (2, [[(105, 105), (105, 115), (115, 115), (115, 105), (105, 105)]], 0);

SELECT grp,
    polygonsEqualsCartesian(
        [groupConvexHull(polygon, should_correct)],
        if(grp = 1,
            readWKTPolygon('POLYGON((0 0,0 10,5 15,15 15,15 5,10 0,0 0))'),
            readWKTPolygon('POLYGON((100 100,100 110,105 115,115 115,115 105,110 100,100 100))')))
FROM geo
GROUP BY grp
ORDER BY grp;

DROP TABLE geo;

-- =============================================================================
-- More shape tests
-- =============================================================================

-- Pentagon with interior points
CREATE TABLE geo (p Point) ENGINE = Memory;
INSERT INTO geo VALUES ((5,0)), ((10,4)), ((8,10)), ((2,10)), ((0,4)), ((5,5)), ((5,3)), ((4,6));
SELECT 'Pentagon with interior:',
    polygonsEqualsCartesian(
        [groupConvexHull(p)],
        readWKTPolygon('POLYGON((0 4,2 10,8 10,10 4,5 0,0 4))'))
FROM geo;
DROP TABLE geo;

-- Polygon with hole: convex hull covers outer ring only
SELECT 'Polygon hole ignored:',
    polygonsEqualsCartesian(
        [groupConvexHull(polygon)],
        readWKTPolygon('POLYGON((0 0,0 20,20 20,20 0,0 0))'))
FROM (SELECT [[(0.,0.),(0.,20.),(20.,20.),(20.,0.),(0.,0.)], [(5.,5.),(5.,15.),(15.,15.),(15.,5.),(5.,5.)]] AS polygon);

-- GROUP BY with Ring input
CREATE TABLE geo (grp Int32, ring Ring) ENGINE = Memory;
INSERT INTO geo VALUES
    (1, [(0, 0), (5, 0), (5, 5), (0, 5), (0, 0)]),
    (1, [(3, 3), (8, 3), (8, 8), (3, 8), (3, 3)]),
    (2, [(10, 10), (15, 10), (15, 15), (10, 15), (10, 10)]);

SELECT grp,
    polygonsEqualsCartesian(
        [groupConvexHull(ring)],
        if(grp = 1,
            readWKTPolygon('POLYGON((0 0,0 5,3 8,8 8,8 3,5 0,0 0))'),
            readWKTPolygon('POLYGON((10 10,10 15,15 15,15 10,10 10))')))
FROM geo
GROUP BY grp
ORDER BY grp;

DROP TABLE geo;

-- =============================================================================
-- Geometry (Variant) input: homogeneous types
-- =============================================================================

CREATE TABLE geo (g Geometry) ENGINE = Memory;
INSERT INTO geo VALUES
    ((0, 0)::Point::Geometry),
    ((10, 0)::Point::Geometry),
    ((10, 10)::Point::Geometry),
    ((0, 10)::Point::Geometry);
SELECT 'Geometry Points:',
    polygonsEqualsCartesian(
        [groupConvexHull(g)],
        readWKTPolygon('POLYGON((0 0,0 10,10 10,10 0,0 0))'))
FROM geo;
DROP TABLE geo;

-- =============================================================================
-- Geometry (Variant) input: heterogeneous types
-- =============================================================================

-- Point + Ring
CREATE TABLE geo (g Geometry) ENGINE = Memory;
INSERT INTO geo VALUES ((0, 0)::Point::Geometry);
INSERT INTO geo VALUES ([(10, 0), (10, 10), (0, 10), (0, 0)]::Ring::Geometry);
SELECT 'Geometry Point+Ring:',
    polygonsEqualsCartesian(
        [groupConvexHull(g)],
        readWKTPolygon('POLYGON((0 0,0 10,10 10,10 0,0 0))'))
FROM geo;
DROP TABLE geo;

-- Point + Polygon
CREATE TABLE geo (g Geometry) ENGINE = Memory;
INSERT INTO geo VALUES ((5, 5)::Point::Geometry);
INSERT INTO geo VALUES ([[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]]::Polygon::Geometry);
SELECT 'Geometry Point+Polygon:',
    polygonsEqualsCartesian(
        [groupConvexHull(g)],
        readWKTPolygon('POLYGON((0 0,0 10,10 10,10 0,0 0))'))
FROM geo;
DROP TABLE geo;

-- Point + Ring + Polygon + MultiPolygon
CREATE TABLE geo (g Geometry) ENGINE = Memory;
INSERT INTO geo VALUES ((0, 0)::Point::Geometry);
INSERT INTO geo VALUES ([(5, 0), (5, 5)]::Ring::Geometry);
INSERT INTO geo VALUES ([[(10, 0), (10, 10), (0, 10), (0, 0)]]::Polygon::Geometry);
INSERT INTO geo VALUES ([[[(20, 0), (20, 20), (0, 20), (0, 0)]]]::MultiPolygon::Geometry);
SELECT 'Geometry all polygon types:',
    polygonsEqualsCartesian(
        [groupConvexHull(g)],
        readWKTPolygon('POLYGON((0 0,0 20,20 20,20 0,0 0))'))
FROM geo;
DROP TABLE geo;

-- Point + LineString
CREATE TABLE geo (g Geometry) ENGINE = Memory;
INSERT INTO geo VALUES ((0, 0)::Point::Geometry);
INSERT INTO geo VALUES (readWKTLineString('LINESTRING (10 0, 10 10, 0 10)')::Geometry);
SELECT 'Geometry Point+LineString:',
    polygonsEqualsCartesian(
        [groupConvexHull(g)],
        readWKTPolygon('POLYGON((0 0,0 10,10 10,10 0,0 0))'))
FROM geo;
DROP TABLE geo;

-- Point + MultiLineString
CREATE TABLE geo (g Geometry) ENGINE = Memory;
INSERT INTO geo VALUES ((0, 0)::Point::Geometry);
INSERT INTO geo VALUES (readWKTMultiLineString('MULTILINESTRING ((10 0, 10 10), (0 10, 0 0))')::Geometry);
SELECT 'Geometry Point+MultiLineString:',
    polygonsEqualsCartesian(
        [groupConvexHull(g)],
        readWKTPolygon('POLYGON((0 0,0 10,10 10,10 0,0 0))'))
FROM geo;
DROP TABLE geo;

-- All six types: Point + Ring + LineString + MultiLineString + Polygon + MultiPolygon
CREATE TABLE geo (g Geometry) ENGINE = Memory;
INSERT INTO geo VALUES ((0, 0)::Point::Geometry);
INSERT INTO geo VALUES ([(5, 0), (5, 5)]::Ring::Geometry);
INSERT INTO geo VALUES (readWKTLineString('LINESTRING (10 0, 10 10)')::Geometry);
INSERT INTO geo VALUES (readWKTMultiLineString('MULTILINESTRING ((0 15, 15 15))')::Geometry);
INSERT INTO geo VALUES ([[(20, 0), (20, 20), (0, 20), (0, 0)]]::Polygon::Geometry);
INSERT INTO geo VALUES ([[[(30, 0), (30, 30), (0, 30), (0, 0)]]]::MultiPolygon::Geometry);
SELECT 'Geometry all six types:',
    polygonsEqualsCartesian(
        [groupConvexHull(g)],
        readWKTPolygon('POLYGON((0 0,0 30,30 30,30 0,0 0))'))
FROM geo;
DROP TABLE geo;

-- Geometry with NULLs (should be skipped)
CREATE TABLE geo (g Geometry) ENGINE = Memory;
INSERT INTO geo VALUES ((0, 0)::Point::Geometry);
INSERT INTO geo VALUES ((10, 0)::Point::Geometry);
INSERT INTO geo VALUES ((10, 10)::Point::Geometry);
SELECT 'Geometry with NULLs:',
    polygonsEqualsCartesian(
        [groupConvexHull(g)],
        readWKTPolygon('POLYGON((0 0,10 0,10 10,0 0))'))
FROM geo;
DROP TABLE geo;

-- =============================================================================
-- Geometry (Variant) input: GROUP BY
-- =============================================================================

CREATE TABLE geo (grp Int32, g Geometry) ENGINE = Memory;
INSERT INTO geo VALUES
    (1, (0, 0)::Point::Geometry),
    (1, (10, 0)::Point::Geometry),
    (1, (10, 10)::Point::Geometry),
    (2, [[(0, 0), (0, 20), (20, 20), (20, 0), (0, 0)]]::Polygon::Geometry),
    (2, (5, 5)::Point::Geometry);

SELECT grp,
    polygonsEqualsCartesian(
        [groupConvexHull(g)],
        if(grp = 1,
            readWKTPolygon('POLYGON((0 0,10 0,10 10,0 0))'),
            readWKTPolygon('POLYGON((0 0,0 20,20 20,20 0,0 0))')))
FROM geo
GROUP BY grp
ORDER BY grp;

DROP TABLE geo;
