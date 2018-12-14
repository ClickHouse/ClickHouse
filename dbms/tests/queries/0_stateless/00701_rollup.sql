DROP TABLE IF EXISTS test.rollup;
CREATE TABLE test.rollup(a String, b Int32, s Int32) ENGINE = Memory;

INSERT INTO test.rollup  VALUES('a', 1, 10);
INSERT INTO test.rollup  VALUES('a', 1, 15);
INSERT INTO test.rollup  VALUES('a', 2, 20);
INSERT INTO test.rollup  VALUES('a', 2, 25);
INSERT INTO test.rollup  VALUES('b', 1, 10);
INSERT INTO test.rollup  VALUES('b', 1, 5);
INSERT INTO test.rollup  VALUES('b', 2, 20);
INSERT INTO test.rollup  VALUES('b', 2, 15);

SELECT a, b, sum(s), count() from test.rollup GROUP BY ROLLUP(a, b) ORDER BY a, b;

SELECT a, b, sum(s), count() from test.rollup GROUP BY ROLLUP(a, b) WITH TOTALS ORDER BY a, b;

SELECT a, sum(s), count() from test.rollup GROUP BY ROLLUP(a) ORDER BY a;

SELECT a, sum(s), count() from test.rollup GROUP BY a WITH ROLLUP ORDER BY a;

SELECT a, sum(s), count() from test.rollup GROUP BY a WITH ROLLUP WITH TOTALS ORDER BY a;
