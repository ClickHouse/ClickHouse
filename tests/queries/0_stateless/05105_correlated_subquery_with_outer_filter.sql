-- A correlated subquery in the `SELECT` list used to fail with 48 `NOT_IMPLEMENTED` ("can't find
-- correlated column") as soon as the outer query had a `WHERE`, unless the correlated column also
-- appeared in the outer `SELECT` list. The column a subquery correlates on is read from the input of the
-- step the subquery belongs to, by the join it is planned into, but no expression of that step reads it,
-- so the step below pruned it.

-- Correlated subqueries are a feature of the analyzer, so this test is skipped without it.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_correlated_filter;
CREATE TABLE t_correlated_filter (id UInt32, v Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_correlated_filter SELECT number, number % 5 FROM numbers(100);

SELECT 'a WHERE that does not reference the correlated column';
SELECT o.id, (SELECT count() FROM t_correlated_filter AS i WHERE i.v = o.v) FROM t_correlated_filter AS o WHERE o.id = 5;

SELECT 'a WHERE on the correlated column itself';
SELECT o.id, (SELECT count() FROM t_correlated_filter AS i WHERE i.v = o.v) FROM t_correlated_filter AS o
WHERE o.v = 0 ORDER BY o.id LIMIT 3;

SELECT 'the correlated column in the SELECT list as well';
SELECT o.id, o.v, (SELECT count() FROM t_correlated_filter AS i WHERE i.v = o.v) FROM t_correlated_filter AS o WHERE o.id = 5;

SELECT 'no WHERE at all';
SELECT count() FROM (
    SELECT o.id, (SELECT count() FROM t_correlated_filter AS i WHERE i.v = o.v) FROM t_correlated_filter AS o);

SELECT 'a correlated EXISTS in the SELECT list';
SELECT o.id, EXISTS (SELECT 1 FROM t_correlated_filter AS i WHERE i.v = o.v AND i.id > 90) FROM t_correlated_filter AS o
WHERE o.id = 5;

SELECT 'PREWHERE';
SELECT o.id, (SELECT count() FROM t_correlated_filter AS i WHERE i.v = o.v) FROM t_correlated_filter AS o PREWHERE o.id = 5;

SELECT 'GROUP BY and HAVING above the filter';
SELECT o.v, count(), (SELECT count() FROM t_correlated_filter AS i WHERE i.v = o.v) FROM t_correlated_filter AS o
WHERE o.id < 30 GROUP BY o.v HAVING count() > 1 ORDER BY o.v;

SELECT 'the subquery in the WHERE itself';
SELECT o.id FROM t_correlated_filter AS o WHERE o.id = 5 AND (SELECT count() FROM t_correlated_filter AS i WHERE i.v = o.v) = 20;

SELECT 'an aggregate of the correlated column';
SELECT o.v, (SELECT max(i.id) FROM t_correlated_filter AS i WHERE i.v = o.v) FROM t_correlated_filter AS o
WHERE o.id < 3 ORDER BY o.v;

DROP TABLE t_correlated_filter;
