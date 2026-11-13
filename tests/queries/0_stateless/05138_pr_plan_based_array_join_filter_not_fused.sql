-- A filter is fused into `ARRAY JOIN` with plan-based parallel replicas, also in a plan fragment shipped to the
-- replicas: only replicas at `DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_PARALLEL_REPLICAS` or later take part,
-- and they all can read the fused filter.

-- Fusion is skipped for serialized plans; pin it so the plan-shape checks below hold in every suite.
SET serialize_query_plan = 0;
-- Pin (randomized in CI): the first check asserts the fusion happens at all.
SET query_plan_fuse_filter_into_array_join = 1;

DROP TABLE IF EXISTS t_pr_fuse;
DROP TABLE IF EXISTS t_pr_fuse_small;

CREATE TABLE t_pr_fuse (id UInt64, arr Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_pr_fuse SELECT number, ['a', 'b', 'c'] FROM numbers(100);

CREATE TABLE t_pr_fuse_small (id UInt64, arr Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_pr_fuse_small SELECT number, ['a', 'b', 'c'] FROM numbers(10);

-- Without parallel replicas the filter is fused into the ARRAY JOIN.
SELECT countIf(explain LIKE '%Element filter%') = 1 AS fused_without_parallel_replicas
FROM (EXPLAIN actions = 1 SELECT id FROM t_pr_fuse ARRAY JOIN arr AS elem WHERE elem = 'b' SETTINGS enable_parallel_replicas = 0);

SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
-- The checks below count lines of the local replica plan and of the shipped fragment, which the
-- `ReadFromParallelReplicas` description prints in full only when it is not cut.
SET parallel_replicas_local_plan = 1;
SET query_plan_max_step_description_length = 1000000;
-- The ARRAY JOIN side has to stay the broadcast one; with the sides swapped the join is not shipped.
SET query_plan_join_swap_table = false;

-- The ARRAY JOIN is not shipped here (only the read is), so the filter stays fused.
SELECT countIf(explain LIKE '%Element filter%') = 1 AS fused_with_parallel_replicas
FROM (EXPLAIN actions = 1 SELECT id FROM t_pr_fuse ARRAY JOIN arr AS elem WHERE elem = 'b');

-- Here the ARRAY JOIN is shipped, on the broadcast side of the join: it appears in the local replica plan
-- and in the fragment, and both have the filter fused. The fragment is printed without actions, so its fused
-- filter is seen as the absence of a separate `Filter (WHERE)` step.
SELECT
    countIf(explain LIKE '%ArrayJoin%') = 2 AS array_join_shipped,
    countIf(explain LIKE '%Element filter%') = 1 AS fused_locally,
    countIf(explain LIKE '%Filter (WHERE)%') = 0 AS fused_in_fragment
FROM (EXPLAIN actions = 1
    SELECT count() FROM t_pr_fuse AS l
    INNER JOIN (SELECT id FROM t_pr_fuse_small ARRAY JOIN arr AS elem WHERE elem = 'b') AS r ON l.id = r.id);

-- The results are the same either way.
SELECT count() FROM (SELECT id FROM t_pr_fuse ARRAY JOIN arr AS elem WHERE elem = 'b');
SELECT count() FROM t_pr_fuse AS l
INNER JOIN (SELECT id FROM t_pr_fuse_small ARRAY JOIN arr AS elem WHERE elem = 'b') AS r ON l.id = r.id;

DROP TABLE t_pr_fuse;
DROP TABLE t_pr_fuse_small;
