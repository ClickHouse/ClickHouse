-- Tags: no-fasttest

-- Test groupPolygonUnion with Ring, Polygon, and MultiPolygon inputs

-- =============================================================================
-- Negative tests
-- =============================================================================

SELECT groupPolygonUnion(42); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonUnion((0, 0)); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonUnion('not a polygon'); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonUnion(polygon, 1, 1)
    FROM (SELECT [[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]] AS polygon); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Geometry variant is not supported
SELECT groupPolygonUnion(g)
    FROM (SELECT [(0.,0.),(1.,0.),(1.,1.),(0.,0.)]::Ring::Geometry AS g); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonUnion(g)
    FROM (SELECT [[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]]::Polygon::Geometry AS g); -- { serverError BAD_ARGUMENTS }
SELECT groupPolygonUnion(g)
    FROM (SELECT (0.,0.)::Point::Geometry AS g); -- { serverError BAD_ARGUMENTS }

-- =============================================================================
-- Ring input
-- =============================================================================

DROP TABLE IF EXISTS geo;
CREATE TABLE geo (ring Ring) ENGINE = Memory;

SELECT 'Ring empty:', wkt(groupPolygonUnion(ring)) FROM geo;

INSERT INTO geo VALUES ([(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]);
SELECT 'Ring single:',
    polygonsEqualsCartesian(
        groupPolygonUnion(ring),
        readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)))'))
FROM geo;

INSERT INTO geo VALUES ([(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]);
SELECT 'Ring union:',
    polygonsEqualsCartesian(
        groupPolygonUnion(ring),
        readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))'))
FROM geo;

DROP TABLE geo;

-- =============================================================================
-- Polygon input
-- =============================================================================

CREATE TABLE geo (polygon Polygon) ENGINE = Memory;

SELECT 'Polygon empty:', wkt(groupPolygonUnion(polygon)) FROM geo;

INSERT INTO geo VALUES ([[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]);
SELECT 'Polygon single:',
    polygonsEqualsCartesian(
        groupPolygonUnion(polygon),
        readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)))'))
FROM geo;

INSERT INTO geo VALUES ([[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]]);
SELECT 'Polygon union:',
    polygonsEqualsCartesian(
        groupPolygonUnion(polygon),
        readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))'))
FROM geo;

DROP TABLE geo;

-- =============================================================================
-- MultiPolygon input
-- =============================================================================

CREATE TABLE geo (mpoly MultiPolygon) ENGINE = Memory;

SELECT 'MultiPolygon empty:', wkt(groupPolygonUnion(mpoly)) FROM geo;

INSERT INTO geo VALUES ([[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]]);
SELECT 'MultiPolygon single:',
    polygonsEqualsCartesian(
        groupPolygonUnion(mpoly),
        readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 5,5 5,5 0,0 0)))'))
FROM geo;

INSERT INTO geo VALUES ([[[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]]]);
SELECT 'MultiPolygon union:',
    polygonsEqualsCartesian(
        groupPolygonUnion(mpoly),
        readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))'))
FROM geo;

DROP TABLE geo;

-- =============================================================================
-- Return type is always MultiPolygon
-- =============================================================================

SELECT 'Ring type:', toTypeName(groupPolygonUnion(r)),
       'Polygon type:', toTypeName(groupPolygonUnion(p)),
       'MultiPolygon type:', toTypeName(groupPolygonUnion(m))
FROM (
    SELECT [(0.,0.),(1.,0.),(1.,1.),(0.,0.)] AS r,
           [[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]] AS p,
           [[[(0.,0.),(1.,0.),(1.,1.),(0.,0.)]]] AS m
);

-- =============================================================================
-- Disjoint polygons remain separate
-- =============================================================================

CREATE TABLE geo (polygon Polygon) ENGINE = Memory;
INSERT INTO geo VALUES
    ([[(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]]),
    ([[(10, 10), (10, 11), (11, 11), (11, 10), (10, 10)]]);
SELECT 'Disjoint:',
    polygonsEqualsCartesian(
        groupPolygonUnion(polygon),
        readWKTMultiPolygon('MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)),((10 10,10 11,11 11,11 10,10 10)))'))
FROM geo;
DROP TABLE geo;

-- =============================================================================
-- GROUP BY
-- =============================================================================

CREATE TABLE geo (grp Int32, polygon Polygon) ENGINE = Memory;
INSERT INTO geo VALUES
    (1, [[(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]]),
    (1, [[(1, 1), (1, 3), (3, 3), (3, 1), (1, 1)]]),
    (2, [[(10, 10), (10, 12), (12, 12), (12, 10), (10, 10)]]);

SELECT grp,
    polygonsEqualsCartesian(
        groupPolygonUnion(polygon),
        if(grp = 1,
            readWKTMultiPolygon('MULTIPOLYGON(((1 2,1 3,3 3,3 1,2 1,2 0,0 0,0 2,1 2)))'),
            readWKTMultiPolygon('MULTIPOLYGON(((10 10,10 12,12 12,12 10,10 10)))')))
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
    ([[(3,3),(3,8),(8,8),(8,3),(3,3)]], 1);
SELECT 'correct_geometry=1:',
    polygonsEqualsCartesian(
        groupPolygonUnion(polygon, should_correct),
        readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))'))
FROM geo;

-- correct_geometry = 0
TRUNCATE TABLE geo;
INSERT INTO geo VALUES
    ([[(0,0),(0,5),(5,5),(5,0),(0,0)]], 0),
    ([[(3,3),(3,8),(8,8),(8,3),(3,3)]], 0);
SELECT 'correct_geometry=0:',
    polygonsEqualsCartesian(
        groupPolygonUnion(polygon, should_correct),
        readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))'))
FROM geo;

-- Mixed correct_geometry values
TRUNCATE TABLE geo;
INSERT INTO geo VALUES
    ([[(0,0),(0,5),(5,5),(5,0),(0,0)]], 1),
    ([[(3,3),(3,8),(8,8),(8,3),(3,3)]], 0);
SELECT 'correct_geometry mixed:',
    polygonsEqualsCartesian(
        groupPolygonUnion(polygon, should_correct),
        readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))'))
FROM geo;

DROP TABLE geo;

-- =============================================================================
-- GROUP BY with correct_geometry
-- =============================================================================

CREATE TABLE geo (grp Int32, polygon Polygon, should_correct UInt8) ENGINE = Memory;
INSERT INTO geo VALUES
    (1, [[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]], 1),
    (1, [[(3, 3), (3, 8), (8, 8), (8, 3), (3, 3)]], 1),
    (2, [[(10, 10), (10, 15), (15, 15), (15, 10), (10, 10)]], 0),
    (2, [[(12, 12), (12, 18), (18, 18), (18, 12), (12, 12)]], 0);

SELECT grp,
    polygonsEqualsCartesian(
        groupPolygonUnion(polygon, should_correct),
        if(grp = 1,
            readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))'),
            readWKTMultiPolygon('MULTIPOLYGON(((12 15,12 18,18 18,18 12,15 12,15 10,10 10,10 15,12 15)))')))
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
    ([(3,3),(3,8),(8,8),(8,3),(3,3)], 1);
SELECT 'Ring with correct_geometry:',
    polygonsEqualsCartesian(
        groupPolygonUnion(ring, should_correct),
        readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))'))
FROM geo;
DROP TABLE geo;

CREATE TABLE geo (mpoly MultiPolygon, should_correct UInt8) ENGINE = Memory;
INSERT INTO geo VALUES
    ([[[(0,0),(0,5),(5,5),(5,0),(0,0)]]], 1),
    ([[[(3,3),(3,8),(8,8),(8,3),(3,3)]]], 1);
SELECT 'MultiPolygon with correct_geometry:',
    polygonsEqualsCartesian(
        groupPolygonUnion(mpoly, should_correct),
        readWKTMultiPolygon('MULTIPOLYGON(((3 5,3 8,8 8,8 3,5 3,5 0,0 0,0 5,3 5)))'))
FROM geo;
DROP TABLE geo;
