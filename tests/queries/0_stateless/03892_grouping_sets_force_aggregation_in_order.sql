-- `force_aggregation_in_order` used to build in-order aggregation state for GROUPING SETS on both
-- planning paths, even though `AggregatingStep::transformPipeline` leaves through the grouping-sets
-- branch before any `AggregatingInOrderTransform` is created.
--
-- Old analyzer: the exception `Trying to get name of not a column: ExpressionList` (issue #97988),
-- because `getSortDescriptionFromGroupBy` called `getColumnName` on each GROUP BY child and with
-- GROUPING SETS those children are `ExpressionList` nodes.
--
-- Analyzer: the stale in-order state reached `AggregatingStep`, so a distributed query with
-- `enable_memory_bound_merging_of_aggregation_results` raised
-- `Memory bound merging of aggregated results is not supported for grouping sets.`

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

-- The analyzer path had the same defect in `Planner::addAggregationStep`. There the stale in-order
-- state is not caught by `getSortDescriptionFromGroupBy`, so it survives into `AggregatingStep` and
-- only surfaces in a distributed query: `enableMemoryBoundMerging` believes the local step is
-- in-order-capable, calls `MergingAggregatedStep::applyOrder`, and the initiator then raises
-- `Memory bound merging of aggregated results is not supported for grouping sets.`

SET enable_analyzer = 1;
SET force_aggregation_in_order = 1;
SET enable_memory_bound_merging_of_aggregation_results = 1;
-- Memory bound merging reads the sort description off the local shard plan, so without a local
-- replica it never engages and the two positive assertions below would silently return 0. The
-- setting is randomized in CI, hence the explicit value (same reason as in 02404_memory_bound_merging).
SET prefer_localhost_replica = 1;

SELECT a, sum(b) FROM remote('127.0.0.{1,2}', currentDatabase(), t_grouping_sets_force)
GROUP BY GROUPING SETS ((), (a)) ORDER BY a;

SELECT a, b, sum(b) FROM remote('127.0.0.{1,2}', currentDatabase(), t_grouping_sets_force)
GROUP BY GROUPING SETS ((a), (b)) ORDER BY a, b;

-- Distributed ROLLUP / CUBE / WITH TOTALS / plain GROUP BY must keep working on that path too.
-- They keep the in-order sort description, which `AggregatingStep::serialize` refuses, so pin
-- serialize_query_plan off: the CI `distributed plan` job turns it on globally. The GROUPING SETS
-- rows above must stay unpinned, they are the ones proving the fix.
SELECT a, sum(b) FROM remote('127.0.0.{1,2}', currentDatabase(), t_grouping_sets_force)
GROUP BY a WITH ROLLUP ORDER BY a SETTINGS serialize_query_plan = 0;
SELECT a, sum(b) FROM remote('127.0.0.{1,2}', currentDatabase(), t_grouping_sets_force)
GROUP BY a WITH CUBE ORDER BY a SETTINGS serialize_query_plan = 0;
SELECT a, sum(b) FROM remote('127.0.0.{1,2}', currentDatabase(), t_grouping_sets_force)
GROUP BY a WITH TOTALS ORDER BY a SETTINGS serialize_query_plan = 0;
SELECT a, sum(b) FROM remote('127.0.0.{1,2}', currentDatabase(), t_grouping_sets_force)
GROUP BY a ORDER BY a SETTINGS serialize_query_plan = 0;

-- Pin the mechanism on the analyzer path as well. Memory-bound merging stays enabled for the
-- sibling GROUP BY modifiers (only GROUPING SETS cannot support it), so a guard that keyed on
-- "any of ROLLUP/CUBE/GROUPING SETS" instead of GROUPING SETS alone would be too wide and would
-- turn the first two rows below into 0.
SELECT 'memory-bound merging kept for distributed plain GROUP BY',
    count() > 0
FROM (EXPLAIN PIPELINE SELECT a, sum(b) FROM remote('127.0.0.{1,2}', currentDatabase(), t_grouping_sets_force) GROUP BY a)
WHERE explain ILIKE '%FinishAggregatingInOrderTransform%';

SELECT 'memory-bound merging kept for distributed ROLLUP',
    count() > 0
FROM (EXPLAIN PIPELINE SELECT a, sum(b) FROM remote('127.0.0.{1,2}', currentDatabase(), t_grouping_sets_force) GROUP BY a WITH ROLLUP)
WHERE explain ILIKE '%FinishAggregatingInOrderTransform%';

SELECT 'memory-bound merging not used for distributed GROUPING SETS',
    count() > 0
FROM (EXPLAIN PIPELINE SELECT a, sum(b) FROM remote('127.0.0.{1,2}', currentDatabase(), t_grouping_sets_force) GROUP BY GROUPING SETS ((), (a)))
WHERE explain ILIKE '%FinishAggregatingInOrderTransform%';

-- The reported query was a mutation subquery, and that carrier reaches the old-analyzer branch even
-- with `enable_analyzer = 1`: `replaceNonDeterministicToScalars` runs `ExecuteScalarSubqueriesVisitor`,
-- which builds an `InterpreterSelectWithUnionQuery` directly. `numbers` keeps the subquery result
-- independent of the mutation, so the value is stable however often the mutation is applied.
DROP TABLE IF EXISTS t_grouping_sets_force_mut;
CREATE TABLE t_grouping_sets_force_mut (c0 Int32, c1 Int32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_grouping_sets_force_mut VALUES (1, 2), (3, 5);

ALTER TABLE t_grouping_sets_force_mut
UPDATE c1 = (SELECT sum(number) FROM numbers(4) GROUP BY GROUPING SETS ((number), ()) ORDER BY 1 DESC LIMIT 1)
WHERE TRUE
SETTINGS enable_analyzer = 0, mutations_execute_subqueries_on_initiator = 1, mutations_sync = 2;

SELECT 'mutation subquery, old analyzer', c0, c1 FROM t_grouping_sets_force_mut ORDER BY c0;

-- `numbers(5)` rather than `numbers(4)`, so this mutation lands a different value than the one
-- above and its execution is observable instead of being implied by an identical assignment.
ALTER TABLE t_grouping_sets_force_mut
UPDATE c1 = (SELECT sum(number) FROM numbers(5) GROUP BY GROUPING SETS ((number), ()) ORDER BY 1 DESC LIMIT 1)
WHERE TRUE
SETTINGS enable_analyzer = 1, mutations_execute_subqueries_on_initiator = 1, mutations_sync = 2;

SELECT 'mutation subquery, analyzer', c0, c1 FROM t_grouping_sets_force_mut ORDER BY c0;

SELECT 'scalar subquery, old analyzer',
    (SELECT sum(number) FROM numbers(4) GROUP BY GROUPING SETS ((number), ()) ORDER BY 1 DESC LIMIT 1)
SETTINGS enable_analyzer = 0;

DROP TABLE t_grouping_sets_force_mut;
DROP TABLE t_grouping_sets_force;
