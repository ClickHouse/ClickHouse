SELECT wkt((0., 0.));
SELECT wkt([(0., 0.), (10., 0.), (10., 10.), (0., 10.)]);
SELECT wkt([[(0., 0.), (10., 0.), (10., 10.), (0., 10.)], [(4., 4.), (5., 4.), (5., 5.), (4., 5.)]]);
SELECT wkt([[[(0., 0.), (10., 0.), (10., 10.), (0., 10.)], [(4., 4.), (5., 4.), (5., 5.), (4., 5.)]], [[(-10., -10.), (-10., -9.), (-9., 10.)]]]);

DROP TABLE IF EXISTS geo;
CREATE TABLE geo (p Tuple(Float64, Float64), id Int) engine=Memory();
INSERT INTO geo VALUES ((0, 0), 1);
INSERT INTO geo VALUES ((1, 0), 2);
INSERT INTO geo VALUES ((2, 0), 3);
SELECT wkt(p) FROM geo ORDER BY id;

DROP TABLE IF EXISTS geo;
CREATE TABLE geo (p Array(Tuple(Float64, Float64)), id Int) engine=Memory();
INSERT INTO geo VALUES ([(0, 0), (10, 0), (10, 10), (0, 10)], 1);
INSERT INTO geo VALUES ([(1, 0), (10, 0), (10, 10), (0, 10)], 2);
INSERT INTO geo VALUES ([(2, 0), (10, 0), (10, 10), (0, 10)], 3);
SELECT wkt(p) FROM geo ORDER BY id;

DROP TABLE IF EXISTS geo;
CREATE TABLE geo (p Array(Array(Tuple(Float64, Float64))), id Int) engine=Memory();
INSERT INTO geo VALUES ([[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (5, 4), (5, 5), (4, 5)]], 1);
INSERT INTO geo VALUES ([[(1, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (5, 4), (5, 5), (4, 5)]], 2);
INSERT INTO geo VALUES ([[(2, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (5, 4), (5, 5), (4, 5)]], 3);
SELECT wkt(p) FROM geo ORDER BY id;

DROP TABLE IF EXISTS geo;
CREATE TABLE geo (p Array(Array(Array(Tuple(Float64, Float64)))), id Int) engine=Memory();
INSERT INTO geo VALUES ([[[(0, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (5, 4), (5, 5), (4, 5)]], [[(-10, -10), (-10, -9), (-9, 10)]]], 1);
INSERT INTO geo VALUES ([[[(1, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (5, 4), (5, 5), (4, 5)]], [[(-10, -10), (-10, -9), (-9, 10)]]], 2);
INSERT INTO geo VALUES ([[[(2, 0), (10, 0), (10, 10), (0, 10)], [(4, 4), (5, 4), (5, 5), (4, 5)]], [[(-10, -10), (-10, -9), (-9, 10)]]], 3);
SELECT wkt(p) FROM geo ORDER BY id;

DROP TABLE geo;
