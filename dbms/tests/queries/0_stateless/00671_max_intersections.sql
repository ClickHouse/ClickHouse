DROP TABLE IF EXISTS test.test1;
DROP TABLE IF EXISTS test.test2;

CREATE TABLE test.test1(start Integer, end Integer) engine = Memory;
CREATE TABLE test.test2(start Integer, end Integer) engine = Memory;

INSERT INTO test.test1(start,end) VALUES (1,3),(3,5);
INSERT INTO test.test2(start,end) VALUES (3,5),(1,3);

SELECT maxIntersections(start,end) from test.test1;
SELECT maxIntersections(start,end) from test.test2;

DROP TABLE test.test1;
DROP TABLE test.test2;
