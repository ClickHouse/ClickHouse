DROP TABLE IF EXISTS polygons;

SELECT 'Const point; No holes';
create table polygons ( id Int32, poly Array(Tuple(Int32, Int32))) engine = Log();

INSERT INTO polygons VALUES (1, [(0, 0), (10, 0), (10, 10), (0, 10)]),
                            (2, [(-5, -5), (5, -5), (5, 5), (-5, 5)]);

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

INSERT INTO polygons VALUES (1, (-9, 0), [(0, 0), (10, 0), (10, 10), (0, 10)]),
                            (2, (-9, 0), [(-5, -5), (5, -5), (5, 5), (-5, 5)]),
                            (3, (0, -9), [(0, 0), (10, 0), (10, 10), (0, 10)]),
                            (4, (0, -9), [(-5, -5), (5, -5), (5, 5), (-5, 5)]),
                            (5, (-4, -4), [(0, 0), (10, 0), (10, 10), (0, 10)]),
                            (6, (-4, -4), [(-5, -5), (5, -5), (5, 5), (-5, 5)]),
                            (7, (0, 0), [(0, 0), (10, 0), (10, 10), (0, 10)]),
                            (8, (0, 0), [(-5, -5), (5, -5), (5, 5), (-5, 5)]),
                            (9, (4, 4), [(0, 0), (10, 0), (10, 10), (0, 10)]),
                            (10, (4, 4), [(-5, -5), (5, -5), (5, 5), (-5, 5)]),
                            (11, (9, 9), [(0, 0), (10, 0), (10, 10), (0, 10)]),
                            (12, (9, 9), [(-5, -5), (5, -5), (5, 5), (-5, 5)]),
                            (13, (9, 4), [(0, 0), (10, 0), (10, 10), (0, 10)]),
                            (14, (9, 4), [(-5, -5), (5, -5), (5, 5), (-5, 5)]),
                            (15, (4, 9), [(0, 0), (10, 0), (10, 10), (0, 10)]),
                            (16, (4, 9), [(-5, -5), (5, -5), (5, 5), (-5, 5)]);

SELECT pointInPolygon(pt, poly) FROM polygons ORDER BY id;

DROP TABLE polygons;

SELECT 'Const point; With holes';

create table polygons ( id Int32, poly Array(Array(Tuple(Int32, Int32)))) engine = Log();

INSERT INTO polygons VALUES (1, [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]),
                            (2, [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);

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

INSERT INTO polygons VALUES (1, (-9, 0), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]),
                            (2, (-9, 0), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]),
                            (3, (0, -9), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]),
                            (4, (0, -9), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]),
                            (5, (-4, -4), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]),
                            (6, (-4, -4), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]),
                            (7, (0, 0), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]),
                            (8, (0, 0), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]),
                            (9, (4, 4), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]),
                            (10, (4, 4), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]),
                            (11, (9, 9), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]),
                            (12, (9, 9), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]),
                            (13, (9, 4), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]),
                            (14, (9, 4), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]),
                            (15, (4, 9), [[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (6, 4), (6, 6), (4, 6)]]),
                            (16, (4, 9), [[(-5, -5), (5, -5), (5, 5), (-5, 5)], [(-1, -1), (1, -1), (1, 1), (-1, 1)]]);

SELECT pointInPolygon(pt, poly) FROM polygons ORDER BY id;

DROP TABLE polygons;
