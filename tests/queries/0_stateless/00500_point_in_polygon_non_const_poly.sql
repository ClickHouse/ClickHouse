DROP TABLE IF EXISTS polygons;

SELECT 'Const point; No holes';
create table polygons ( id Int32, poly Array(Tuple(Int32, Int32))) engine = Log();

INSERT INTO polygons VALUES (1, [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (2, [(-5, -5), (5, -5), (5, 5), (-5, 5)]);

SELECT pointInPolygon((-9, 0), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((0, -9), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((-4, -4), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((0, 0), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((4, 4), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((9, 9), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((9, 4), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((4, 9), poly) FROM polygons ORDER BY id;

DROP TABLE polygons;

SELECT 'Non-const point; No holes';

create table polygons ( id Int32, pt Tuple(Int32, Int32), poly Array(Tuple(Int32, Int32))) engine = Log();

INSERT INTO polygons VALUES (1, (-9, 0), [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (2, (-9, 0), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);
INSERT INTO polygons VALUES (3, (0, -9), [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (4, (0, -9), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);
INSERT INTO polygons VALUES (5, (-4, -4), [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (6, (-4, -4), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);
INSERT INTO polygons VALUES (7, (0, 0), [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (8, (0, 0), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);
INSERT INTO polygons VALUES (9, (4, 4), [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (10, (4, 4), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);
INSERT INTO polygons VALUES (11, (9, 9), [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (12, (9, 9), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);
INSERT INTO polygons VALUES (13, (9, 4), [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (14, (9, 4), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);
INSERT INTO polygons VALUES (15, (4, 9), [(0, 0), (10, 0), (10, 10), (0, 10)]);
INSERT INTO polygons VALUES (16, (4, 9), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);

SELECT pointInPolygon(pt, poly) FROM polygons ORDER BY id;

DROP TABLE polygons;

SELECT 'Const point; With holes';

create table polygons ( id Int32, poly Array(Array(Tuple(Int32, Int32)))) engine = Log();

INSERT INTO polygons VALUES (1, [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (2, [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);

SELECT pointInPolygon((-9, 0), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((0, -9), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((-4, -4), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((0, 0), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((4, 4), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((9, 9), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((9, 4), poly) FROM polygons ORDER BY id;
SELECT pointInPolygon((4, 9), poly) FROM polygons ORDER BY id;

DROP TABLE polygons;

SELECT 'Non-const point; With holes';

create table polygons ( id Int32, pt Tuple(Int32, Int32), poly Array(Array(Tuple(Int32, Int32)))) engine = Log();

INSERT INTO polygons VALUES (1, (-9, 0), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (2, (-9, 0), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);
INSERT INTO polygons VALUES (3, (0, -9), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (4, (0, -9), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);
INSERT INTO polygons VALUES (5, (-4, -4), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (6, (-4, -4), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);
INSERT INTO polygons VALUES (7, (0, 0), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (8, (0, 0), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);
INSERT INTO polygons VALUES (9, (4, 4), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (10, (4, 4), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);
INSERT INTO polygons VALUES (11, (9, 9), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (12, (9, 9), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);
INSERT INTO polygons VALUES (13, (9, 4), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (14, (9, 4), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);
INSERT INTO polygons VALUES (15, (4, 9), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]);
INSERT INTO polygons VALUES (16, (4, 9), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);

SELECT pointInPolygon(pt, poly) FROM polygons ORDER BY id;

DROP TABLE polygons;
