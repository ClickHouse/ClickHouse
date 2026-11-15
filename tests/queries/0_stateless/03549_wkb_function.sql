DROP TABLE IF EXISTS geom1;
CREATE TABLE IF NOT EXISTS geom1 (a Point) ENGINE = Memory();
INSERT INTO geom1 VALUES((0, 0));
INSERT INTO geom1 VALUES((0, 20));
INSERT INTO geom1 VALUES((10, 20));
SELECT hex(wkb(a)) FROM geom1 ORDER BY ALL;

DROP TABLE IF EXISTS geom2;
CREATE TABLE IF NOT EXISTS geom2 (a LineString) ENGINE = Memory();
INSERT INTO geom2 VALUES([]);
INSERT INTO geom2 VALUES([(0, 0), (10, 0), (10, 10), (0, 10)]);
SELECT hex(wkb(a)) FROM geom2 ORDER BY ALL;

DROP TABLE IF EXISTS geom3;
CREATE TABLE IF NOT EXISTS geom3 (a Polygon) ENGINE = Memory();
INSERT INTO geom3 VALUES([]);
INSERT INTO geom3 VALUES([[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]]);
SELECT hex(wkb(a)) FROM geom3 ORDER BY ALL;

DROP TABLE IF EXISTS geom4;
CREATE TABLE IF NOT EXISTS geom4 (a MultiLineString) ENGINE = Memory();
INSERT INTO geom4 VALUES([]);
INSERT INTO geom4 VALUES([[(0, 0), (10, 0), (10, 10), (0, 10)], [(1, 1), (2, 2), (3, 3)]]);
SELECT hex(wkb(a)) FROM geom4 ORDER BY ALL;

DROP TABLE IF EXISTS geom5;
CREATE TABLE IF NOT EXISTS geom5 (a MultiPolygon) ENGINE = Memory();
INSERT INTO geom5 VALUES([]);
INSERT INTO geom5 VALUES([[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]]);
SELECT hex(wkb(a)) FROM geom5 ORDER BY ALL;

