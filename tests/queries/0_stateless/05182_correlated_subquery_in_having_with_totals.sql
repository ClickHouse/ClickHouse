-- A correlated subquery in `HAVING` used to fail with 10 `NOT_FOUND_COLUMN_IN_BLOCK` when the query used
-- `WITH TOTALS`: `TotalsHavingStep` evaluates `HAVING` itself, and unlike the ordinary filter step it did
-- not decorrelate the subqueries of `HAVING` first, so their result stayed an unresolved input of the
-- `HAVING` expression.

-- Correlated subqueries are a feature of the analyzer, so this test is skipped without it.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_correlated_having_totals;
CREATE TABLE t_correlated_having_totals (id UInt32, v Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_correlated_having_totals SELECT number, number % 3 FROM numbers(20);

SELECT 'a correlated scalar subquery in HAVING WITH TOTALS';
SELECT o.v, count() FROM t_correlated_having_totals AS o
GROUP BY o.v WITH TOTALS
HAVING (SELECT count() FROM t_correlated_having_totals AS i WHERE i.v = o.v) > 6
ORDER BY o.v;

SELECT 'the same HAVING without the correlated subquery, for comparison';
SELECT o.v, count() FROM t_correlated_having_totals AS o
GROUP BY o.v WITH TOTALS
HAVING count() > 6
ORDER BY o.v;

SELECT 'the correlated column is not in the SELECT list';
SELECT count() FROM t_correlated_having_totals AS o
GROUP BY o.v WITH TOTALS
HAVING (SELECT count() FROM t_correlated_having_totals AS i WHERE i.v = o.v) > 6
ORDER BY count();

SELECT 'a correlated EXISTS in HAVING WITH TOTALS';
SELECT o.v, count() FROM t_correlated_having_totals AS o
GROUP BY o.v WITH TOTALS
HAVING EXISTS (SELECT 1 FROM t_correlated_having_totals AS i WHERE i.v = o.v AND i.id > 18)
ORDER BY o.v;

SELECT 'with totals_mode = after_having_exclusive';
SELECT o.v, count() FROM t_correlated_having_totals AS o
GROUP BY o.v WITH TOTALS
HAVING (SELECT count() FROM t_correlated_having_totals AS i WHERE i.v = o.v) > 6
ORDER BY o.v
SETTINGS totals_mode = 'after_having_exclusive';

SELECT 'with totals_mode = after_having_inclusive';
SELECT o.v, count() FROM t_correlated_having_totals AS o
GROUP BY o.v WITH TOTALS
HAVING (SELECT count() FROM t_correlated_having_totals AS i WHERE i.v = o.v) > 6
ORDER BY o.v
SETTINGS totals_mode = 'after_having_inclusive';

SELECT 'with totals_mode = before_having';
SELECT o.v, count() FROM t_correlated_having_totals AS o
GROUP BY o.v WITH TOTALS
HAVING (SELECT count() FROM t_correlated_having_totals AS i WHERE i.v = o.v) > 6
ORDER BY o.v
SETTINGS totals_mode = 'before_having';

SELECT 'the same, without the correlated subquery';
SELECT o.v, count() FROM t_correlated_having_totals AS o
GROUP BY o.v WITH TOTALS
HAVING count() > 6
ORDER BY o.v
SETTINGS totals_mode = 'before_having';

SELECT 'an outer WHERE prunes the correlated column as well';
SELECT count() FROM t_correlated_having_totals AS o
WHERE o.id < 9
GROUP BY o.v WITH TOTALS
HAVING (SELECT count() FROM t_correlated_having_totals AS i WHERE i.v = o.v) > 6
ORDER BY count();

DROP TABLE t_correlated_having_totals;
