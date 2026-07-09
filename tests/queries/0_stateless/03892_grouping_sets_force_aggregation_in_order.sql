-- Grouping sets with force_aggregation_in_order used to abort the server with
-- "Trying to get name of not a column: ExpressionList" (issue #97988): the old
-- analyzer's force_aggregation_in_order branch called getColumnName() on each
-- GROUP BY child, but with GROUPING SETS those children are ExpressionList nodes.

SET enable_analyzer = 0;
SET force_aggregation_in_order = 1;

DROP TABLE IF EXISTS t_grouping_sets_force;
CREATE TABLE t_grouping_sets_force (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_grouping_sets_force VALUES (1, 2), (3, 4), (1, 5);

SELECT a, sum(b) FROM t_grouping_sets_force GROUP BY GROUPING SETS ((), (a)) ORDER BY a;

SELECT a, b, sum(b) FROM t_grouping_sets_force
GROUP BY GROUPING SETS ((a), (b), (a, b), ())
ORDER BY a, b;

-- ROLLUP / CUBE / plain GROUP BY on the same branch must keep working.
SELECT a, sum(b) FROM t_grouping_sets_force GROUP BY a WITH ROLLUP ORDER BY a;
SELECT a, sum(b) FROM t_grouping_sets_force GROUP BY a WITH CUBE ORDER BY a;
SELECT a, sum(b) FROM t_grouping_sets_force GROUP BY a ORDER BY a;

DROP TABLE t_grouping_sets_force;
