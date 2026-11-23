-- A correlated scalar subquery used as the whole filter is validated in its own scope, and the outer columns it uses are read.
-- https://github.com/ClickHouse/ClickHouse/issues/124499

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_corr_whole_filter;
CREATE TABLE t_corr_whole_filter (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_corr_whole_filter SELECT number % 3, number FROM numbers(6);

SELECT o.number FROM numbers(1) AS o WHERE (SELECT count() > 0 FROM numbers(1) AS i WHERE i.number = o.number);

SELECT 'WHERE';
SELECT o.v FROM t_corr_whole_filter AS o WHERE (SELECT count() > 0 FROM t_corr_whole_filter AS i WHERE i.k = o.k AND i.v > 4) ORDER BY o.v;
SELECT o.v FROM t_corr_whole_filter AS o WHERE (SELECT sum(i.v) > 4 FROM t_corr_whole_filter AS i WHERE i.k = o.k) ORDER BY o.v;
SELECT o.v FROM t_corr_whole_filter AS o WHERE (SELECT grouping(i.k) = 0 FROM t_corr_whole_filter AS i WHERE i.k = o.k GROUP BY i.k) ORDER BY o.v;
SELECT o.v FROM t_corr_whole_filter AS o WHERE (SELECT i.v > 4 FROM t_corr_whole_filter AS i WHERE i.k = o.k AND i.v = o.v) ORDER BY o.v;
SELECT o.v FROM t_corr_whole_filter AS o WHERE (SELECT count() OVER () > 0 FROM t_corr_whole_filter AS i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }

SELECT 'PREWHERE';
SELECT o.v FROM t_corr_whole_filter AS o PREWHERE (SELECT sum(i.v) > 4 FROM t_corr_whole_filter AS i WHERE i.k = o.k); -- { serverError ILLEGAL_PREWHERE }

SELECT 'HAVING';
SELECT o.k, count() FROM t_corr_whole_filter AS o GROUP BY o.k HAVING (SELECT sum(i.v) > 4 FROM t_corr_whole_filter AS i WHERE i.k = o.k) ORDER BY o.k;
SELECT o.k, count() FROM t_corr_whole_filter AS o GROUP BY o.k HAVING (SELECT sum(i.v) > 4 FROM t_corr_whole_filter AS i WHERE i.k = o.k) ORDER BY o.k SETTINGS analyzer_compatibility_allow_non_aggregate_in_having = 1;
-- A column of the outer query that is not a GROUP BY key is still rejected.
SELECT count() FROM t_corr_whole_filter AS o GROUP BY o.k HAVING (SELECT o.v > 0); -- { serverError NOT_AN_AGGREGATE }

SELECT 'QUALIFY';
SELECT o.k, count() FROM t_corr_whole_filter AS o GROUP BY o.k QUALIFY (SELECT sum(i.v) > 4 FROM t_corr_whole_filter AS i WHERE i.k = o.k) ORDER BY o.k;
SELECT o.v FROM t_corr_whole_filter AS o QUALIFY (SELECT sum(i.v) > 4 FROM t_corr_whole_filter AS i WHERE i.k = o.k) ORDER BY o.v;

DROP TABLE t_corr_whole_filter;
