-- Regression test: the DP join-order optimizer must keep running when parallel replicas is enabled.
-- Previously `QueryPlanOptimizationSettings` forced `query_plan_optimize_join_order_limit=0`
-- whenever `allow_experimental_parallel_reading_from_replicas && max_parallel_replicas > 1`, which
-- silently skipped join reordering for any PR query.

-- For runs with old analyzer

DROP TABLE IF EXISTS pr_dp_a;
DROP TABLE IF EXISTS pr_dp_b;
DROP TABLE IF EXISTS pr_dp_c;

CREATE TABLE pr_dp_a (x UInt32) ENGINE = MergeTree ORDER BY x AS SELECT number % 100   FROM numbers(100);
CREATE TABLE pr_dp_b (x UInt32) ENGINE = MergeTree ORDER BY x AS SELECT number % 1000  FROM numbers(1000);
CREATE TABLE pr_dp_c (x UInt32) ENGINE = MergeTree ORDER BY x AS SELECT number % 100   FROM numbers(100000);

SET
  enable_parallel_replicas = 1,
  cluster_for_parallel_replicas = 'parallel_replicas',
  max_parallel_replicas = 3,
  parallel_replicas_for_non_replicated_merge_tree = 1,
  parallel_replicas_local_plan = 1,
  query_plan_join_swap_table = 'auto',
  query_plan_optimize_join_order_algorithm = 'dpsize',
  query_plan_optimize_join_order_limit = 10,
  automatic_parallel_replicas_mode = 0;

-- Three-way join written as `(a JOIN b) JOIN c` with `a` (100 rows) < `b` (1000) < `c` (100000).
-- The 2-table swap can also go through the legacy `optimizeJoinLegacy` path, so this test forces
-- `query_plan_optimize_join_order_algorithm='dpsize'` and uses a 3-way join to exercise the actual
-- DP optimizer (`query_graph_size_limit > 2` in `optimizeJoinLogicalImpl`).
SELECT count() FROM pr_dp_a AS a JOIN pr_dp_b AS b ON a.x = b.x JOIN pr_dp_c AS c ON b.x = c.x
SETTINGS log_comment = '04277_dp_reorder';

SYSTEM FLUSH LOGS query_log;

-- `JoinReorderMicroseconds` is the reordering algorithm's own timer: non-zero whenever DP runs, and
-- exactly 0 with `query_plan_optimize_join_order_limit = 0`, the behaviour parallel replicas used to
-- force. Asserting it next to `ParallelReplicasQueryCount` checks what the query executed rather
-- than how its plan prints, so it covers both implementations of parallel replicas. The plan-based
-- one carries the shipped fragment in the read step description, and an `EXPLAIN` filtered by step
-- name reads that fragment's own read step as an extra row.
SELECT ProfileEvents['ParallelReplicasQueryCount'] > 0, ProfileEvents['JoinReorderMicroseconds'] > 0
FROM system.query_log
WHERE log_comment = '04277_dp_reorder' AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND is_initial_query
  AND event_date >= yesterday() AND event_time >= now() - INTERVAL 30 MINUTE
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE pr_dp_a;
DROP TABLE pr_dp_b;
DROP TABLE pr_dp_c;
