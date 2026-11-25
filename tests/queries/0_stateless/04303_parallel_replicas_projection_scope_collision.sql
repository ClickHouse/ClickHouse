-- Companion to 04278_parallel_replicas_coordinator_mode_collision, for projections.
--
-- The outer query enables projections (`optimize_use_projections = 1`); the subquery pins them OFF.
-- Under parallel replicas this is a per-subquery settings divergence between the initiator and the
-- replicas: the initiator's local plan is optimized as part of the outer plan, so it picks up the
-- outer `optimize_use_projections = 1` and reads the aggregate projection, while each remote replica
-- re-plans the subquery standalone with its own `optimize_use_projections = 0` and reads the raw
-- parts. Unlike the read-in-order decision, the projection choice is not announced to the shared
-- coordinator, so it does not throw; this test just checks that the split still returns the correct
-- result (no double-counting of the parts covered by the projection).

DROP TABLE IF EXISTS t_pr_proj_scope;

CREATE TABLE t_pr_proj_scope (id UInt64, key UInt64, value UInt64, PROJECTION p_agg (SELECT key, sum(value) GROUP BY key))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;

-- Two parts so the projection split across replicas is non-trivial.
INSERT INTO t_pr_proj_scope SELECT number, number % 100, number FROM numbers(100000);
INSERT INTO t_pr_proj_scope SELECT number, number % 100, number FROM numbers(100000, 100000);

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1;

-- Expected total: sum(value) over numbers(0, 200000) = 19999900000.
SELECT sum(s)
FROM
(
    SELECT key, sum(value) AS s FROM t_pr_proj_scope GROUP BY key
    SETTINGS optimize_use_projections = 0
)
SETTINGS optimize_use_projections = 1;

DROP TABLE t_pr_proj_scope;
