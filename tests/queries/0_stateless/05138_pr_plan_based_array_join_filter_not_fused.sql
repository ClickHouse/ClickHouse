-- Plan-based parallel replicas ships the plan fragment to the replicas, so fusing a filter into
-- `ARRAY JOIN` has to be suppressed for the same reason as under `make_distributed_plan` and
-- `serialize_query_plan`: `ArrayJoinStep` writes its element filter without a serialization version bump,
-- on the assumption that a fused filter never leaves the initiator. The filter also carries an `ActionsDAG`,
-- which is one more place an `IN` set could hide from the shipping checks.

SET enable_analyzer = 1;
-- Fusion is skipped for serialized plans; pin it so the plan-shape checks below hold in every suite.
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_pr_fuse;

CREATE TABLE t_pr_fuse (id UInt64, arr Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_pr_fuse SELECT number, ['a', 'b', 'c'] FROM numbers(100);

-- Without parallel replicas the filter is fused into the ARRAY JOIN.
SELECT countIf(explain LIKE '%Element filter%') = 1 AS fused_without_parallel_replicas
FROM (EXPLAIN actions = 1 SELECT id FROM t_pr_fuse ARRAY JOIN arr AS elem WHERE elem = 'b');

SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- With plan-based parallel replicas it is not.
SELECT countIf(explain LIKE '%Element filter%') = 0 AS not_fused_with_parallel_replicas
FROM (EXPLAIN actions = 1 SELECT id FROM t_pr_fuse ARRAY JOIN arr AS elem WHERE elem = 'b');

-- The result is the same either way.
SELECT count() FROM (SELECT id FROM t_pr_fuse ARRAY JOIN arr AS elem WHERE elem = 'b');

DROP TABLE t_pr_fuse;
