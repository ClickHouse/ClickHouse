-- Tests that pointInPolygon accepts the named geometric data types Ring, Polygon,
-- MultiPolygon and Geometry, in addition to the raw Array(Tuple(...)) forms, and that
-- the result matches the equivalent raw representation.
-- https://github.com/ClickHouse/ClickHouse/issues/109848

SELECT '-- Ring (constant) --';
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]::Ring);
SELECT pointInPolygon((100., 100.), [(6, 0), (8, 4), (5, 8), (0, 2)]::Ring);
-- A named Ring gives the same answer as the raw array of tuples.
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]::Ring)
     = pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]);

SELECT '-- Polygon (constant, with a hole) --';
-- Outer square (0,0)-(10,10) with a square hole (4,4)-(6,6).
SELECT pointInPolygon((5., 5.), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]::Polygon); -- in the hole -> 0
SELECT pointInPolygon((2., 2.), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]::Polygon); -- inside, outside the hole -> 1
SELECT pointInPolygon((20., 20.), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]::Polygon); -- outside -> 0

SELECT '-- MultiPolygon (constant) --';
SELECT pointInPolygon((2., 2.), [[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (30, 20), (30, 30), (20, 30)]]]::MultiPolygon); -- in the first -> 1
SELECT pointInPolygon((25., 25.), [[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (30, 20), (30, 30), (20, 30)]]]::MultiPolygon); -- in the second -> 1
SELECT pointInPolygon((15., 15.), [[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (30, 20), (30, 30), (20, 30)]]]::MultiPolygon); -- in neither -> 0

SELECT '-- Ring column --';
DROP TABLE IF EXISTS pip_ring;
CREATE TABLE pip_ring (id Int32, pt Point, r Ring) ENGINE = Memory;
INSERT INTO pip_ring VALUES (1, (3, 3), [(6, 0), (8, 4), (5, 8), (0, 2)]), (2, (100, 100), [(6, 0), (8, 4), (5, 8), (0, 2)]);
SELECT id, pointInPolygon(pt, r) FROM pip_ring ORDER BY id;
DROP TABLE pip_ring;

SELECT '-- Polygon column --';
DROP TABLE IF EXISTS pip_poly;
CREATE TABLE pip_poly (id Int32, pt Point, pg Polygon) ENGINE = Memory;
INSERT INTO pip_poly VALUES (1, (2, 2), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]), (2, (5, 5), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
SELECT id, pointInPolygon(pt, pg) FROM pip_poly ORDER BY id;
DROP TABLE pip_poly;

SELECT '-- MultiPolygon column --';
DROP TABLE IF EXISTS pip_mp;
CREATE TABLE pip_mp (id Int32, pt Point, mpg MultiPolygon) ENGINE = Memory;
INSERT INTO pip_mp VALUES (1, (2, 2), [[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (30, 20), (30, 30), (20, 30)]]]), (2, (25, 25), [[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (30, 20), (30, 30), (20, 30)]]]), (3, (15, 15), [[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (30, 20), (30, 30), (20, 30)]]]);
SELECT id, pointInPolygon(pt, mpg) FROM pip_mp ORDER BY id;
DROP TABLE pip_mp;

SELECT '-- Geometry column (mixed Ring, Polygon and MultiPolygon values) --';
DROP TABLE IF EXISTS pip_geo;
CREATE TABLE pip_geo (id Int32, geom Geometry) ENGINE = Memory;
INSERT INTO pip_geo VALUES (1, readWKT('POLYGON((0 0, 10 0, 10 10, 0 10))'));
INSERT INTO pip_geo VALUES (2, readWKT('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10)), ((20 20, 30 20, 30 30, 20 30)))'));
INSERT INTO pip_geo VALUES (3, CAST([(0, 0), (10, 0), (10, 10), (0, 10)] AS Ring)::Geometry);
SELECT id, pointInPolygon((5., 5.), geom) FROM pip_geo ORDER BY id;
SELECT id, pointInPolygon((25., 25.), geom) FROM pip_geo ORDER BY id;
DROP TABLE pip_geo;

SELECT '-- Geometry from readWKT (constant) --';
SELECT pointInPolygon((3., 3.), readWKT('POLYGON((6 0, 8 4, 5 8, 0 2))'));
