-- A `SQL SECURITY DEFINER` / `NONE` view that can hide rows is a barrier for the
-- per-partition rewrite family too. `optimizeDistinctPerPartition` /
-- `optimizeLimitByPerPartition` / `optimizeAggregationPerPartition` walk down through
-- `ExpressionStep` and `FilterStep` and call
-- `ReadFromMergeTree::requestOutputEachPartitionThroughSeparatePort*`, and
-- `applyStreamDisjointness` propagates the resulting partition disjointness up the plan.
-- Both crossed the view's sealing step, so an outer `DISTINCT`, `LIMIT BY` or `GROUP BY`
-- retuned the read scheduling and the stream merging inside the view - progress, timing
-- and resource usage below the seal then depend on the rows the view drops. Both walks now
-- fail closed on a barrier step.

-- The three `allow_*_partitions_independently` settings are deliberately left at their
-- defaults (all `1`): the point of the test is that the rewrites are live and still do not
-- cross the barrier. Everything else the plan shape depends on is pinned, because the test
-- also runs with randomized settings; none of these affects what the barrier guards.
SET max_threads = 8,
    enable_parallel_replicas = 0, make_distributed_plan = 0,
    max_rows_to_group_by = 0, max_rows_in_distinct = 0, max_bytes_in_distinct = 0,
    explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t04837;
CREATE TABLE t04837 (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
INSERT INTO t04837 SELECT number % 16, number FROM numbers(1000);

-- A view that drops rows, so it is a barrier, and its twin that is not.
CREATE VIEW v04837_invoker SQL SECURITY INVOKER AS SELECT a, b FROM t04837 WHERE b != 42;
CREATE VIEW v04837_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT a, b FROM t04837 WHERE b != 42;

-- The `INVOKER` twin is the positive control: the rewrite is applied and the markers appear.
-- For the `DEFINER` view no marker may appear at all. Before the fix both sides were identical.
SELECT 'invoker twin, outer DISTINCT:', trim(explain)
FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM v04837_invoker)
WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%separate port%';

SELECT 'definer, outer DISTINCT does not reach the reading:', count()
FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM v04837_definer)
WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%separate port%';

SELECT 'invoker twin, outer LIMIT BY:', trim(explain)
FROM (EXPLAIN actions = 1 SELECT a FROM v04837_invoker LIMIT 1 BY a)
WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%separate port%';

SELECT 'definer, outer LIMIT BY does not reach the reading:', count()
FROM (EXPLAIN actions = 1 SELECT a FROM v04837_definer LIMIT 1 BY a)
WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%separate port%';

-- The barrier only drops the optimization, never the correctness of the result.
SELECT 'definer view results:',
       (SELECT count() FROM (SELECT DISTINCT a FROM v04837_definer)) = (SELECT count() FROM (SELECT DISTINCT a FROM v04837_invoker)),
       (SELECT count() FROM (SELECT a FROM v04837_definer LIMIT 1 BY a)) = (SELECT count() FROM (SELECT a FROM v04837_invoker LIMIT 1 BY a));

-- The same contract with the old analyzer, where the view is read through `StorageView::read`.
SET enable_analyzer = 0;

SELECT 'old analyzer, invoker twin, outer DISTINCT:', trim(explain)
FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM v04837_invoker)
WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%separate port%';

SELECT 'old analyzer, definer, outer DISTINCT does not reach the reading:', count()
FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM v04837_definer)
WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%separate port%';

SET enable_analyzer = DEFAULT;

DROP VIEW v04837_invoker;
DROP VIEW v04837_definer;

-- The second half of the family: the view's own inner `DISTINCT` legitimately makes its
-- reading output each partition through a separate port, and `applyStreamDisjointness` then
-- carried that property up through the sealing step, so the invoker's outer `GROUP BY` and
-- `LIMIT BY` skipped their stream merging according to how the rows the view hides are
-- distributed over the partitions. The property must stop at the seal.
CREATE VIEW d04837_invoker SQL SECURITY INVOKER AS SELECT DISTINCT a, b FROM t04837 WHERE b != 42;
CREATE VIEW d04837_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT DISTINCT a, b FROM t04837 WHERE b != 42;

SELECT 'invoker twin, outer GROUP BY over a distinct view:', trim(explain)
FROM (EXPLAIN actions = 1 SELECT a, count() FROM d04837_invoker GROUP BY a)
WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%separate port%';

SELECT 'definer, disjointness does not cross the seal:', count()
FROM (EXPLAIN actions = 1 SELECT a, count() FROM d04837_definer GROUP BY a)
WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%separate port%';

SELECT 'definer, disjointness does not cross the seal, outer LIMIT BY:', count()
FROM (EXPLAIN actions = 1 SELECT a FROM d04837_definer LIMIT 1 BY a)
WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%separate port%';

SELECT 'definer view results:',
       (SELECT count() FROM (SELECT a, count() FROM d04837_definer GROUP BY a)) = (SELECT count() FROM (SELECT a, count() FROM d04837_invoker GROUP BY a)),
       (SELECT count() FROM (SELECT a FROM d04837_definer LIMIT 1 BY a)) = (SELECT count() FROM (SELECT a FROM d04837_invoker LIMIT 1 BY a));

DROP VIEW d04837_invoker;
DROP VIEW d04837_definer;
DROP TABLE t04837;
