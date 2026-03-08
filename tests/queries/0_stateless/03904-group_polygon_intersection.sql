-- Tags: no-fasttest

-- Test groupPolygonIntersection with Ring, Polygon, and MultiPolygon inputs

-- =============================================================================
-- Negative tests
-- =============================================================================

SELECT groupPolygonIntersection(42); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonIntersection((0, 0)); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonIntersection('not a polygon'); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonIntersection(polygon, polygon, polygon)
    FROM (SELECT [[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]] AS polygon); -- { serverError BAD_ARGUMENTS }

-- Invalid correct_geometry type
SELECT groupPolygonIntersection(polygon, polygon)
    FROM (SELECT [[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]] AS polygon); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonIntersection(polygon, 1.5)
    FROM (SELECT [[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]] AS polygon); -- { serverError BAD_ARGUMENTS }

-- Geometry variant is not supported
SELECT groupPolygonIntersection(g)
    FROM (SELECT [(0.,0.),(1.,0.),(1.,1.),(0.,0.)]::Ring::Geometry AS g); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonIntersection(g)
    FROM (SELECT [[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]]::Polygon::Geometry AS g); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonIntersection(g)
    FROM (SELECT (0.,0.)::Point::Geometry AS g); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- Ring input
-- =============================================================================

DROP TABLE IF EXISTS geo;
CREATE TABLE geo (ring Ring) ENGINE = Memory;

SELECT 'Ring empty:', wkt(groupPolygonIntersection(ring)) FROM geo;

INSERT INTO geo VALUES ([(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]);
SELECT 'Ring single:',
    polygonsEqualsCartesian(
        groupPolygonIntersection(ring),
        readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)))'))
FROM geo;

INSERT INTO geo VALUES ([(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]);
SELECT 'Ring two overlapping:',
    polygonsEqualsCartesian(
        groupPolygonIntersection(ring),
        readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))'))
FROM geo;

DROP TABLE geo;

-- =============================================================================
-- Polygon input
-- =============================================================================

CREATE TABLE geo (polygon Polygon) ENGINE = Memory;

SELECT 'Polygon empty:', wkt(groupPolygonIntersection(polygon)) FROM geo;

INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]);
SELECT 'Polygon single:',
    polygonsEqualsCartesian(
        groupPolygonIntersection(polygon),
        readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)))'))
FROM geo;

INSERT INTO geo VALUES ([[(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]]);
SELECT 'Polygon two overlapping:',
    polygonsEqualsCartesian(
        groupPolygonIntersection(polygon),
        readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))'))
FROM geo;

DROP TABLE geo;

-- =============================================================================
-- MultiPolygon input
-- =============================================================================

CREATE TABLE geo (mpoly MultiPolygon) ENGINE = Memory;

SELECT 'MultiPolygon empty:', wkt(groupPolygonIntersection(mpoly)) FROM geo;

INSERT INTO geo VALUES ([[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]]);
SELECT 'MultiPolygon single:',
    polygonsEqualsCartesian(
        groupPolygonIntersection(mpoly),
        readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)))'))
FROM geo;

INSERT INTO geo VALUES ([[[(2, 2), (2, 7), (7, 7), (7, 2), (2, 2)]]]);
SELECT 'MultiPolygon two overlapping:',
    polygonsEqualsCartesian(
        groupPolygonIntersection(mpoly),
        readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))'))
FROM geo;

DROP TABLE geo;

-- =============================================================================
-- Return type is always MultiPolygon
-- =============================================================================

SELECT 'Ring type:', toTypeName(groupPolygonIntersection(r)),
       'Polygon type:', toTypeName(groupPolygonIntersection(p)),
       'MultiPolygon type:', toTypeName(groupPolygonIntersection(m))
FROM (
    SELECT [(0.,0.),(1.,0.),(1.,1.),(0.,0.)] AS r,
           [[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]] AS p,
           [[[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]]] AS m
);

-- =============================================================================
-- Disjoint polygons => empty intersection
-- =============================================================================

CREATE TABLE geo (polygon Polygon) ENGINE = Memory;
INSERT INTO geo VALUES
    ([[(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]]),
    ([[(10, 10), (10, 11), (11, 11), (11, 10), (10, 10)]]);
SELECT 'Disjoint:', wkt(groupPolygonIntersection(polygon)) FROM geo;
DROP TABLE geo;

-- =============================================================================
-- GROUP BY
-- =============================================================================

CREATE TABLE geo (grp Int32, polygon Polygon) ENGINE = Memory;
INSERT INTO geo VALUES
    (1, [[(0, 0), (0, 4), (4, 4), (4, 0), (0, 0)]]),
    (1, [[(1, 1), (1, 5), (5, 5), (5, 1), (1, 1)]]),
    (1, [[(2, 2), (2, 6), (6, 6), (6, 2), (2, 2)]]),
    (2, [[(10, 10), (10, 14), (14, 14), (14, 10), (10, 10)]]),
    (2, [[(11, 11), (11, 15), (15, 15), (15, 11), (11, 11)]]);

SELECT grp,
    polygonsEqualsCartesian(
        groupPolygonIntersection(polygon),
        if(grp = 1,
            readWKTMultiPolygon('MULTIPOLYGON(((2 4,4 4,4 2,2 2,2 4)))'),
            readWKTMultiPolygon('MULTIPOLYGON(((11 14,14 14,14 11,11 11,11 14)))')))
FROM geo
GROUP BY grp
ORDER BY grp;

DROP TABLE geo;

-- =============================================================================
-- correct_geometry parameter
-- =============================================================================

CREATE TABLE geo (polygon Polygon, should_correct UInt8) ENGINE = Memory;

-- correct_geometry = 1
TRUNCATE TABLE geo;
INSERT INTO geo VALUES
    ([[(0,0),(0,5),(5,5),(5,0),(0,0)]], 1),
    ([[(2,2),(2,7),(7,7),(7,2),(2,2)]], 1);
SELECT 'correct_geometry=1:',
    polygonsEqualsCartesian(
        groupPolygonIntersection(polygon, should_correct),
        readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))'))
FROM geo;

-- correct_geometry = 0
TRUNCATE TABLE geo;
INSERT INTO geo VALUES
    ([[(0,0),(0,5),(5,5),(5,0),(0,0)]], 0),
    ([[(2,2),(2,7),(7,7),(7,2),(2,2)]], 0);
SELECT 'correct_geometry=0:',
    polygonsEqualsCartesian(
        groupPolygonIntersection(polygon, should_correct),
        readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))'))
FROM geo;

-- Mixed correct_geometry values
TRUNCATE TABLE geo;
INSERT INTO geo VALUES
    ([[(0,0),(0,5),(5,5),(5,0),(0,0)]], 1),
    ([[(2,2),(2,7),(7,7),(7,2),(2,2)]], 0);
SELECT 'correct_geometry mixed:',
    polygonsEqualsCartesian(
        groupPolygonIntersection(polygon, should_correct),
        readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))'))
FROM geo;

DROP TABLE geo;

-- =============================================================================
-- More intersection scenarios
-- =============================================================================

-- Three overlapping polygons
CREATE TABLE geo (polygon Polygon) ENGINE = Memory;
INSERT INTO geo VALUES
    ([[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]]),
    ([[(2, 2), (2, 12), (12, 12), (12, 2), (2, 2)]]),
    ([[(4, 4), (4, 14), (14, 14), (14, 4), (4, 4)]]);
SELECT 'Three overlapping:',
    polygonsEqualsCartesian(
        groupPolygonIntersection(polygon),
        readWKTMultiPolygon('MULTIPOLYGON(((4 10,10 10,10 4,4 4,4 10)))'))
FROM geo;
DROP TABLE geo;

-- Partial overlap
CREATE TABLE geo (polygon Polygon) ENGINE = Memory;
INSERT INTO geo VALUES
    ([[(0, 0), (0, 6), (6, 6), (6, 0), (0, 0)]]),
    ([[(3, 3), (3, 9), (9, 9), (9, 3), (3, 3)]]);
SELECT 'Partial overlap:',
    polygonsEqualsCartesian(
        groupPolygonIntersection(polygon),
        readWKTMultiPolygon('MULTIPOLYGON(((3 6,6 6,6 3,3 3,3 6)))'))
FROM geo;
DROP TABLE geo;

-- Small squares
CREATE TABLE geo (polygon Polygon) ENGINE = Memory;
INSERT INTO geo VALUES
    ([[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]]),
    ([[(5, 5), (5, 15), (15, 15), (15, 5), (5, 5)]]);
SELECT 'Small squares:',
    polygonsEqualsCartesian(
        groupPolygonIntersection(polygon),
        readWKTMultiPolygon('MULTIPOLYGON(((5 10,10 10,10 5,5 5,5 10)))'))
FROM geo;
DROP TABLE geo;

-- Single geometry with correct_geometry => unchanged
SELECT 'Single with correct_geometry:',
    polygonsEqualsCartesian(
        groupPolygonIntersection(polygon, 1),
        readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)))'))
FROM (SELECT [[(0., 0.), (0., 5.), (5., 5.), (5., 0.), (0., 0.)]] AS polygon);

-- Empty table with correct_geometry
CREATE TABLE geo (polygon Polygon) ENGINE = Memory;
SELECT 'Empty with correct_geometry:', wkt(groupPolygonIntersection(polygon, 1)) FROM geo;
DROP TABLE geo;

-- =============================================================================
-- GROUP BY with correct_geometry
-- =============================================================================

CREATE TABLE geo (grp Int32, polygon Polygon, should_correct UInt8) ENGINE = Memory;
INSERT INTO geo VALUES
    (1, [[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]], 1),
    (1, [[(2, 2), (2, 12), (12, 12), (12, 2), (2, 2)]], 1),
    (2, [[(5, 5), (5, 15), (15, 15), (15, 5), (5, 5)]], 0),
    (2, [[(7, 7), (7, 17), (17, 17), (17, 7), (7, 7)]], 0);

SELECT grp,
    polygonsEqualsCartesian(
        groupPolygonIntersection(polygon, should_correct),
        if(grp = 1,
            readWKTMultiPolygon('MULTIPOLYGON(((2 10,10 10,10 2,2 2,2 10)))'),
            readWKTMultiPolygon('MULTIPOLYGON(((7 15,15 15,15 7,7 7,7 15)))')))
FROM geo
GROUP BY grp
ORDER BY grp;

DROP TABLE geo;

-- =============================================================================
-- Ring and MultiPolygon with correct_geometry
-- =============================================================================

CREATE TABLE geo (ring Ring, should_correct UInt8) ENGINE = Memory;
INSERT INTO geo VALUES
    ([(0,0),(0,5),(5,5),(5,0),(0,0)], 1),
    ([(2,2),(2,7),(7,7),(7,2),(2,2)], 1);
SELECT 'Ring with correct_geometry:',
    polygonsEqualsCartesian(
        groupPolygonIntersection(ring, should_correct),
        readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))'))
FROM geo;
DROP TABLE geo;

CREATE TABLE geo (mpoly MultiPolygon, should_correct UInt8) ENGINE = Memory;
INSERT INTO geo VALUES
    ([[[(0,0),(0,5),(5,5),(5,0),(0,0)]]], 1),
    ([[[(2,2),(2,7),(7,7),(7,2),(2,2)]]], 1);
SELECT 'MultiPolygon with correct_geometry:',
    polygonsEqualsCartesian(
        groupPolygonIntersection(mpoly, should_correct),
        readWKTMultiPolygon('MULTIPOLYGON(((2 5,5 5,5 2,2 2,2 5)))'))
FROM geo;
DROP TABLE geo;
