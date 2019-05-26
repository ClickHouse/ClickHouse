DROP TABLE IF EXISTS test1;
DROP TABLE IF EXISTS test2;

CREATE TABLE test1(start Integer, end Integer) engine = Memory;
CREATE TABLE test2(start Integer, end Integer) engine = Memory;

INSERT INTO test1(start,end) VALUES (1,3),(3,5);
INSERT INTO test2(start,end) VALUES (3,5),(1,3);

SELECT maxIntersections(start,end) from test1;
SELECT maxIntersections(start,end) from test2;

DROP TABLE test1;
DROP TABLE test2;
