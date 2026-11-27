-- Tags: no-random-merge-tree-settings

-- `ORDER BY ... WITH FILL` under plan-based parallel replicas (`parallel_replicas_plan_based`).
--
-- The sort is shipped with the fragment, and its description used to be rejected by
-- `serializeSortDescription`, so every `WITH FILL` query failed with
-- "WITH FILL is not supported in serialized sort description". The description travels in full now,
-- bounds included. The fill itself still runs on the initiator: `FillingStep` is added only on the
-- finalizing node, above the merge, so a replica just returns its rows in order.
-- See https://github.com/ClickHouse/ClickHouse/issues/115527

DROP TABLE IF EXISTS t_pr_with_fill;

CREATE TABLE t_pr_with_fill (a UInt64, b String)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8;

-- Gaps of 4 between the keys, so `WITH FILL STEP 2` has something to fill in every gap.
INSERT INTO t_pr_with_fill SELECT number * 4, toString(number) FROM numbers(100);
OPTIMIZE TABLE t_pr_with_fill FINAL;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_plan_based = 1;
-- Pin the manual mode: CI randomizes `automatic_parallel_replicas_mode` to 2, and the cost model may then
-- decide against parallel replicas, so the plan-based split would never engage.
SET automatic_parallel_replicas_mode = 0;

-- The filled stream must be identical to non-parallel execution. Aggregating it keeps the reference small
-- while still failing if a gap is filled twice or left unfilled.
SELECT '--- WITH FILL STEP 2, local ---';
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_with_fill ORDER BY a WITH FILL STEP 2)
SETTINGS enable_parallel_replicas = 0;
SELECT '--- WITH FILL STEP 2, plan_based = 1 ---';
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_with_fill ORDER BY a WITH FILL STEP 2);

-- The head of that stream, so the order of the filled rows is checked and not only their total.
SELECT '--- WITH FILL STEP 2, head, local ---';
SELECT a FROM t_pr_with_fill ORDER BY a WITH FILL STEP 2 LIMIT 8 SETTINGS enable_parallel_replicas = 0;
SELECT '--- WITH FILL STEP 2, head, plan_based = 1 ---';
SELECT a FROM t_pr_with_fill ORDER BY a WITH FILL STEP 2 LIMIT 8;

-- `LIMIT` counts filled rows, and the shipped per-replica top-N is derived from it, so a replica must still
-- return enough real rows for the initiator to fill up to the limit.
SELECT '--- WITH FILL STEP 2 LIMIT 7 OFFSET 3, local ---';
SELECT a FROM t_pr_with_fill ORDER BY a WITH FILL STEP 2 LIMIT 7 OFFSET 3 SETTINGS enable_parallel_replicas = 0;
SELECT '--- WITH FILL STEP 2 LIMIT 7 OFFSET 3, plan_based = 1 ---';
SELECT a FROM t_pr_with_fill ORDER BY a WITH FILL STEP 2 LIMIT 7 OFFSET 3;

-- `FROM` and `TO` add rows outside the range the replicas returned.
SELECT '--- WITH FILL FROM 2 TO 30 STEP 3, local ---';
SELECT a FROM t_pr_with_fill WHERE a < 20 ORDER BY a WITH FILL FROM 2 TO 30 STEP 3
SETTINGS enable_parallel_replicas = 0;
SELECT '--- WITH FILL FROM 2 TO 30 STEP 3, plan_based = 1 ---';
SELECT a FROM t_pr_with_fill WHERE a < 20 ORDER BY a WITH FILL FROM 2 TO 30 STEP 3;

-- `INTERPOLATE` carries a column value into the generated rows, which only works if the rows reach the
-- initiator in order.
SELECT '--- WITH FILL STEP 2 INTERPOLATE, local ---';
SELECT a, b FROM t_pr_with_fill ORDER BY a WITH FILL STEP 2 INTERPOLATE (b AS b) LIMIT 8
SETTINGS enable_parallel_replicas = 0;
SELECT '--- WITH FILL STEP 2 INTERPOLATE, plan_based = 1 ---';
SELECT a, b FROM t_pr_with_fill ORDER BY a WITH FILL STEP 2 INTERPOLATE (b AS b) LIMIT 8;

-- `ORDER BY count() WITH FILL LIMIT n` over a `GROUP BY` is the shape `tryPushBucketTopKIntoAggregation`
-- refuses when the sort description carries `with_fill`, so the flag has to survive the wire for the
-- fragment's own re-optimization to see it (the fragment's aggregation is not final, so the optimization
-- would not fire either way - this pins the result, not the plan).
-- The four groups have distinct sizes (10, 20, 30, 40), so the order of the top-N is not tie-dependent.
SELECT '--- GROUP BY, ORDER BY count() WITH FILL, local ---';
SELECT k, c FROM (
    SELECT multiIf(a < 40, 0, a < 120, 1, a < 240, 2, 3) AS k, count() AS c
    FROM t_pr_with_fill GROUP BY k ORDER BY c WITH FILL LIMIT 8)
SETTINGS enable_parallel_replicas = 0;
SELECT '--- GROUP BY, ORDER BY count() WITH FILL, plan_based = 1 ---';
SELECT k, c FROM (
    SELECT multiIf(a < 40, 0, a < 120, 1, a < 240, 2, 3) AS k, count() AS c
    FROM t_pr_with_fill GROUP BY k ORDER BY c WITH FILL LIMIT 8);

-- The build side of a broadcast join is executed in full by every replica, so a `WITH FILL` subquery
-- there travels inside the fragment (and has to be clonable and serializable). Each replica fills the
-- same complete build side, so the join result must not change.
DROP TABLE IF EXISTS t_pr_with_fill_probe;
CREATE TABLE t_pr_with_fill_probe (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_with_fill_probe SELECT number FROM numbers(400);

SELECT '--- join over a filled build side, local ---';
SELECT count(), sum(p.a) FROM t_pr_with_fill_probe AS p
ALL INNER JOIN (SELECT a FROM t_pr_with_fill ORDER BY a WITH FILL STEP 2) AS f ON p.a = f.a
SETTINGS enable_parallel_replicas = 0;
SELECT '--- join over a filled build side, plan_based = 1 ---';
SELECT count(), sum(p.a) FROM t_pr_with_fill_probe AS p
ALL INNER JOIN (SELECT a FROM t_pr_with_fill ORDER BY a WITH FILL STEP 2) AS f ON p.a = f.a;

DROP TABLE t_pr_with_fill_probe;

-- The sort really is shipped now - the query is not silently kept local.
SELECT '--- explain: has_remote_read, sort_shipped ---';
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%merge sorted streams from replicas%') > 0 AS sort_shipped
FROM (EXPLAIN pretty = 0 SELECT a FROM t_pr_with_fill ORDER BY a WITH FILL STEP 2);

DROP TABLE t_pr_with_fill;
