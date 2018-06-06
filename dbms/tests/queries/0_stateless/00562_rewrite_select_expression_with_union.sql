DROP TABLE IF EXISTS test.test;

CREATE TABLE test.test ( s String,  i Int64) ENGINE = Memory;

INSERT INTO test.test VALUES('test_string', 1);

SELECT s, SUM(i*2) AS i FROM test.test GROUP BY s  UNION ALL  SELECT s, SUM(i*2) AS i FROM test.test GROUP BY s;
SELECT s FROM (SELECT s, SUM(i*2) AS i FROM test.test GROUP BY s  UNION ALL  SELECT s, SUM(i*2) AS i FROM test.test GROUP BY s);

DROP TABLE IF EXISTS test.test;
