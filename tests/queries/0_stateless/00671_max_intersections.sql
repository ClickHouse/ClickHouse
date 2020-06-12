DROP TABLE IF EXISTS test1_00671;
DROP TABLE IF EXISTS test2_00671;

CREATE TABLE test1_00671(start Integer, end Integer) engine = Memory;
CREATE TABLE test2_00671(start Integer, end Integer) engine = Memory;

INSERT INTO test1_00671(start,end) VALUES (1,3),(3,5);
INSERT INTO test2_00671(start,end) VALUES (3,5),(1,3);

SELECT maxIntersections(start,end) from test1_00671;
SELECT maxIntersections(start,end) from test2_00671;

DROP TABLE test1_00671;
DROP TABLE test2_00671;
