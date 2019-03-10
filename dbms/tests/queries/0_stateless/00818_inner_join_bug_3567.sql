USE test;

DROP TABLE IF EXISTS test.using1;
DROP TABLE IF EXISTS test.using2;

CREATE TABLE test.using1(a String, b DateTime) ENGINE=MergeTree order by a;
CREATE TABLE test.using2(c String, a String, d DateTime) ENGINE=MergeTree order by c;

INSERT INTO test.using1 VALUES ('a', '2018-01-01 00:00:00') ('b', '2018-01-01 00:00:00') ('c', '2018-01-01 00:00:00');
INSERT INTO test.using2 VALUES ('d', 'd', '2018-01-01 00:00:00') ('b', 'b', '2018-01-01 00:00:00') ('c', 'c', '2018-01-01 00:00:00');

SELECT * FROM test.using1 t1 ALL LEFT JOIN (SELECT *, c as a, d as b FROM test.using2) t2 USING (a, b) ORDER BY d;
SELECT * FROM test.using1 t1 ALL INNER JOIN (SELECT *, c as a, d as b FROM test.using2) t2 USING (a, b) ORDER BY d;

DROP TABLE test.using1;
DROP TABLE test.using2;
