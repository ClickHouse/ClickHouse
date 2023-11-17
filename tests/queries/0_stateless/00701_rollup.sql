-- Tags: no-parallel, no-fasttest

DROP TABLE IF EXISTS rollup;
CREATE TABLE rollup(a String, b Int32, s Int32) ENGINE = Memory;

INSERT INTO rollup VALUES ('a', 1, 10), ('a', 1, 15), ('a', 2, 20);
INSERT INTO rollup VALUES ('a', 2, 25), ('b', 1, 10), ('b', 1, 5);
INSERT INTO rollup VALUES ('b', 2, 20), ('b', 2, 15);

SELECT a, b, sum(s), count() from rollup GROUP BY ROLLUP(a, b) ORDER BY a, b;

SELECT a, b, sum(s), count() from rollup GROUP BY ROLLUP(a, b) WITH TOTALS ORDER BY a, b;

SELECT a, sum(s), count() from rollup GROUP BY ROLLUP(a) ORDER BY a;

SELECT a, sum(s), count() from rollup GROUP BY a WITH ROLLUP ORDER BY a;

SELECT a, sum(s), count() from rollup GROUP BY a WITH ROLLUP WITH TOTALS ORDER BY a;

SET group_by_two_level_threshold = 1;

SELECT a, sum(s), count() from rollup GROUP BY a WITH ROLLUP ORDER BY a;
SELECT a, b, sum(s), count() from rollup GROUP BY a, b WITH ROLLUP ORDER BY a, b;

DROP TABLE rollup;
