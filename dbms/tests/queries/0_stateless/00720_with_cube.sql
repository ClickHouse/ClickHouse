DROP TABLE IF EXISTS rollup;
CREATE TABLE rollup(a String, b Int32, s Int32) ENGINE = Memory;

INSERT INTO rollup  VALUES('a', 1, 10);
INSERT INTO rollup  VALUES('a', 1, 15);
INSERT INTO rollup  VALUES('a', 2, 20);
INSERT INTO rollup  VALUES('a', 2, 25);
INSERT INTO rollup  VALUES('b', 1, 10);
INSERT INTO rollup  VALUES('b', 1, 5);
INSERT INTO rollup  VALUES('b', 2, 20);
INSERT INTO rollup  VALUES('b', 2, 15);

SELECT a, b, sum(s), count() from rollup GROUP BY CUBE(a, b) ORDER BY a, b;

SELECT a, b, sum(s), count() from rollup GROUP BY CUBE(a, b) WITH TOTALS ORDER BY a, b;

SELECT a, b, sum(s), count() from rollup GROUP BY a, b WITH CUBE ORDER BY a;

SELECT a, b, sum(s), count() from rollup GROUP BY a, b WITH CUBE WITH TOTALS ORDER BY a;
