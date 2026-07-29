-- A query with GROUPING SETS and `force_aggregation_in_order` used to raise the exception
-- `Trying to get name of not a column: ExpressionList` (issue #97988): the old analyzer's
-- `force_aggregation_in_order` branch called `getColumnName` on each GROUP BY child, but with
-- GROUPING SETS those children are `ExpressionList` nodes.

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

-- The result rows above are identical whether or not in-order aggregation is used, so pin the
-- mechanism too: `force_aggregation_in_order` must still reach `AggregatingInOrderTransform` for
-- the sibling forms and must not for GROUPING SETS. `optimize_aggregation_in_order` is off so
-- that only the forced path can introduce the transform.
SET optimize_aggregation_in_order = 0;

SELECT 'in-order forced for plain GROUP BY',
    count() > 0
FROM (EXPLAIN PIPELINE SELECT a, sum(b) FROM t_grouping_sets_force GROUP BY a)
WHERE explain ILIKE '%AggregatingInOrderTransform%';

SELECT 'in-order forced for ROLLUP',
    count() > 0
FROM (EXPLAIN PIPELINE SELECT a, sum(b) FROM t_grouping_sets_force GROUP BY a WITH ROLLUP)
WHERE explain ILIKE '%AggregatingInOrderTransform%';

SELECT 'in-order forced for CUBE',
    count() > 0
FROM (EXPLAIN PIPELINE SELECT a, sum(b) FROM t_grouping_sets_force GROUP BY a WITH CUBE)
WHERE explain ILIKE '%AggregatingInOrderTransform%';

SELECT 'in-order not forced for GROUPING SETS',
    count() > 0
FROM (EXPLAIN PIPELINE SELECT a, sum(b) FROM t_grouping_sets_force GROUP BY GROUPING SETS ((), (a)))
WHERE explain ILIKE '%AggregatingInOrderTransform%';

DROP TABLE t_grouping_sets_force;
