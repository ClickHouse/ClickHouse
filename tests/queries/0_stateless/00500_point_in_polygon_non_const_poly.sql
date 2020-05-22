DROP TABLE IF EXISTS polygons;

SELECT 'Const point; No holes';
create table polygons ( id Int32, poly Array(Tuple(Int32, Int32))) engine = Log();

INSERT INTO polygons VALUES (1, [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (2, [(-5, -5), (5, -5), (5, 5), (-5, 5)]);

SELECT pointInPolygon((-10, 0), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((0, -10), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((-5, -5), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((0, 0), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((5, 5), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((10, 10), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((10, 5), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((5, 10), poly) FROM polygons ORDER BY id;

DROP TABLE polygons;

SELECT 'Non-const point; No holes';

create table polygons ( id Int32, pt Tuple(Int32, Int32), poly Array(Tuple(Int32, Int32))) engine = Log();

INSERT INTO polygons VALUES (1, (-10, 0), [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (2, (-10, 0), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);
INSERT INTO polygons VALUES (3, (0, -10), [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (4, (0, -10), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);
INSERT INTO polygons VALUES (5, (-5, -5), [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (6, (-5, -5), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);
INSERT INTO polygons VALUES (7, (0, 0), [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (8, (0, 0), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);
INSERT INTO polygons VALUES (9, (5, 5), [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (10, (5, 5), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);
INSERT INTO polygons VALUES (11, (10, 10), [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (12, (10, 10), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);
INSERT INTO polygons VALUES (13, (10, 5), [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (14, (10, 5), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);
INSERT INTO polygons VALUES (15, (5, 10), [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (16, (5, 10), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);

SELECT pointInPolygon(pt, poly) FROM polygons ORDER BY id;

DROP TABLE polygons;

SELECT 'Const point; With holes';

create table polygons ( id Int32, poly Array(Array(Tuple(Int32, Int32)))) engine = Log();

INSERT INTO polygons VALUES (1, [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (2, [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);

SELECT pointInPolygon((-10, 0), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((0, -10), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((-5, -5), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((0, 0), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((5, 5), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((10, 10), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((10, 5), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((5, 10), poly) FROM polygons ORDER BY id;

DROP TABLE polygons;

SELECT 'Non-const point; With holes';

create table polygons ( id Int32, pt Tuple(Int32, Int32), poly Array(Array(Tuple(Int32, Int32)))) engine = Log();

INSERT INTO polygons VALUES (1, (-10, 0), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (2, (-10, 0), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);
INSERT INTO polygons VALUES (3, (0, -10), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (4, (0, -10), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);
INSERT INTO polygons VALUES (5, (-5, -5), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (6, (-5, -5), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);
INSERT INTO polygons VALUES (7, (0, 0), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (8, (0, 0), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);
INSERT INTO polygons VALUES (9, (5, 5), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (10, (5, 5), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);
INSERT INTO polygons VALUES (11, (10, 10), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (12, (10, 10), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);
INSERT INTO polygons VALUES (13, (10, 5), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (14, (10, 5), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);
INSERT INTO polygons VALUES (15, (5, 10), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (16, (5, 10), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);

SELECT pointInPolygon(pt, poly) FROM polygons ORDER BY id;

DROP TABLE polygons;