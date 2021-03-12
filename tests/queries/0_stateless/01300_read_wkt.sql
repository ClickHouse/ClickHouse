SELECT readWktPoint('POINT(0 0)');
SELECT readWktPolygon('POLYGON((1 0,10 0,10 10,0 10,1 0))');
SELECT readWktPolygon('POLYGON((0 0,10 0,10 10,0 10,0 0),(4 4,5 4,5 5,4 5,4 4))');
SELECT readWktMultiPolygon('MULTIPOLYGON(((2 0,10 0,10 10,0 10,2 0),(4 4,5 4,5 5,4 5,4 4)),((-10 -10,-10 -9,-9 10,-10 -10)))');

DROP TABLE IF EXISTS geo;
CREATE TABLE geo (s String, id Int) engine=Memory();
INSERT INTO geo VALUES ('POINT(0 0)', 1);
INSERT INTO geo VALUES ('POINT(1 0)', 2);
INSERT INTO geo VALUES ('POINT(2 0)', 3);
SELECT readWktPoint(s) FROM geo ORDER BY id;

DROP TABLE IF EXISTS geo;
CREATE TABLE geo (s String, id Int) engine=Memory();
INSERT INTO geo VALUES ('POLYGON((1 0,10 0,10 10,0 10,1 0))', 1);
INSERT INTO geo VALUES ('POLYGON((0 0,10 0,10 10,0 10,0 0))', 2);
INSERT INTO geo VALUES ('POLYGON((2 0,10 0,10 10,0 10,2 0))', 3);
INSERT INTO geo VALUES ('POLYGON((0 0,10 0,10 10,0 10,0 0),(4 4,5 4,5 5,4 5,4 4))', 4);
INSERT INTO geo VALUES ('POLYGON((2 0,10 0,10 10,0 10,2 0),(4 4,5 4,5 5,4 5,4 4))', 5);
INSERT INTO geo VALUES ('POLYGON((1 0,10 0,10 10,0 10,1 0),(4 4,5 4,5 5,4 5,4 4))', 6);
SELECT readWktPolygon(s) FROM geo ORDER BY id;

DROP TABLE IF EXISTS geo;
CREATE TABLE geo (s String, id Int) engine=Memory();
INSERT INTO geo VALUES ('MULTIPOLYGON(((1 0,10 0,10 10,0 10,1 0),(4 4,5 4,5 5,4 5,4 4)),((-10 -10,-10 -9,-9 10,-10 -10)))', 1);
INSERT INTO geo VALUES ('MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0),(4 4,5 4,5 5,4 5,4 4)),((-10 -10,-10 -9,-9 10,-10 -10)))', 2);
INSERT INTO geo VALUES ('MULTIPOLYGON(((2 0,10 0,10 10,0 10,2 0),(4 4,5 4,5 5,4 5,4 4)),((-10 -10,-10 -9,-9 10,-10 -10)))', 3);
SELECT readWktMultiPolygon(s) FROM geo ORDER BY id;
