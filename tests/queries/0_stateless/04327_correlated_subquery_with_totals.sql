SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;

DROP TABLE IF EXISTS t_04327;
CREATE TABLE t_04327 (id UInt32, val Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_04327 SELECT number, number * 7 FROM numbers(3);

-- A correlated subquery whose result carries a totals stream cannot be decorrelated:
-- the totals would leak through the decorrelation JOIN into the outer non-aggregate query.

-- Scalar correlated subquery with WITH TOTALS.
SELECT id FROM t_04327 WHERE id >= (SELECT 0 GROUP BY 1 WITH TOTALS HAVING isNull(val) = 0); -- { serverError NOT_IMPLEMENTED }

-- Same via EXISTS.
SELECT id FROM t_04327 WHERE EXISTS (SELECT 0 GROUP BY 1 WITH TOTALS HAVING isNull(val) = 0); -- { serverError NOT_IMPLEMENTED }

-- Totals produced by a nested subquery that is not consumed by an aggregation.
SELECT id FROM t_04327 WHERE id >= (SELECT x FROM (SELECT 0 AS x GROUP BY 1 WITH TOTALS) AS s WHERE t_04327.val >= 0); -- { serverError NOT_IMPLEMENTED }

-- A nested WITH TOTALS consumed by an enclosing aggregation does not leak and is allowed.
SELECT id FROM t_04327 WHERE id >= (SELECT max(x) FROM (SELECT number AS x FROM numbers(3) GROUP BY number WITH TOTALS) WHERE t_04327.val >= 0) ORDER BY id;

-- WITH ROLLUP / WITH CUBE produce ordinary rows, not a totals stream, and are allowed.
SELECT id FROM t_04327 WHERE id >= (SELECT 0 GROUP BY 1 WITH ROLLUP HAVING isNull(val) = 0) ORDER BY id;

-- The same shape without WITH TOTALS is decorrelated normally.
SELECT id FROM t_04327 WHERE id >= (SELECT 0 GROUP BY 1 HAVING isNull(val) = 0) ORDER BY id;

DROP TABLE t_04327;
