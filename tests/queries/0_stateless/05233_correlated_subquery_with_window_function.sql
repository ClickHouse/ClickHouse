-- A correlated subquery next to a window function and in `QUALIFY`, correlating on a column the
-- query itself does not select.

DROP TABLE IF EXISTS t_correlated_window;
SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS t_correlated_window;
CREATE TABLE t_correlated_window (id UInt32, v Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_correlated_window SELECT number, number % 4 FROM numbers(10);

SELECT 'a correlated subquery in the SELECT list next to a window function';
SELECT row_number() OVER (ORDER BY o.id) AS rn, (SELECT count() FROM t_correlated_window AS i WHERE i.v = o.v) AS c
FROM t_correlated_window AS o ORDER BY rn;

SELECT 'the same with a plain window function, for comparison';
SELECT row_number() OVER (ORDER BY o.id) AS rn, count() OVER (PARTITION BY o.v) AS c
FROM t_correlated_window AS o ORDER BY rn;

SELECT 'an outer WHERE below the window function';
SELECT row_number() OVER (ORDER BY o.id) AS rn, (SELECT count() FROM t_correlated_window AS i WHERE i.v = o.v) AS c
FROM t_correlated_window AS o WHERE o.id < 6 ORDER BY rn;

SELECT 'a correlated subquery in QUALIFY';
SELECT o.id, row_number() OVER (ORDER BY o.id) AS rn
FROM t_correlated_window AS o
QUALIFY (SELECT count() FROM t_correlated_window AS i WHERE i.v = o.v) > 2
ORDER BY o.id;

SELECT 'the same QUALIFY with a plain window function, for comparison';
SELECT o.id, row_number() OVER (ORDER BY o.id) AS rn
FROM t_correlated_window AS o
QUALIFY count() OVER (PARTITION BY o.v) > 2
ORDER BY o.id;

SELECT 'a correlated EXISTS in QUALIFY';
SELECT o.id, max(o.id) OVER (ORDER BY o.id) AS m
FROM t_correlated_window AS o
QUALIFY EXISTS (SELECT 1 FROM t_correlated_window AS i WHERE i.v = o.v AND i.id > 7)
ORDER BY o.id;

SELECT 'a correlated subquery in the SELECT list with a QUALIFY below it';
SELECT o.id, (SELECT count() FROM t_correlated_window AS i WHERE i.v = o.v) AS c
FROM t_correlated_window AS o
QUALIFY row_number() OVER (ORDER BY o.id) < 4
ORDER BY o.id;

DROP TABLE t_correlated_window;
