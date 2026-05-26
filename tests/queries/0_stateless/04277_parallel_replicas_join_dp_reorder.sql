-- Regression test: the DP join-order optimizer must keep running when parallel replicas is enabled.
-- Previously `QueryPlanOptimizationSettings` forced `query_plan_optimize_join_order_limit=0`
-- whenever `allow_experimental_parallel_reading_from_replicas && max_parallel_replicas > 1`, which
-- silently skipped join reordering for any PR query.

DROP TABLE IF EXISTS pr_dp_a;
DROP TABLE IF EXISTS pr_dp_b;
DROP TABLE IF EXISTS pr_dp_c;

CREATE TABLE pr_dp_a (x UInt32) ENGINE = MergeTree ORDER BY x AS SELECT number % 100   FROM numbers(100);
CREATE TABLE pr_dp_b (x UInt32) ENGINE = MergeTree ORDER BY x AS SELECT number % 1000  FROM numbers(1000);
CREATE TABLE pr_dp_c (x UInt32) ENGINE = MergeTree ORDER BY x AS SELECT number % 100   FROM numbers(100000);

-- Three-way join written as `(a JOIN b) JOIN c` with `a` (100 rows) < `b` (1000) < `c` (100000).
-- The 2-table swap can also go through the legacy `optimizeJoinLegacy` path, so this test forces
-- `query_plan_optimize_join_order_algorithm='dpsize'` and uses a 3-way join to exercise the actual
-- DP optimizer (`query_graph_size_limit > 2` in `optimizeJoinLogicalImpl`).
--
-- With DP disabled (`query_plan_optimize_join_order_limit=0`) `optimizeJoinLogicalImpl` exits
-- early without converting the `JoinStepLogical`s, and `optimizeJoinLegacy` is a no-op on them, so
-- the user-written order `(a JOIN b) JOIN c` reaches `ReadFromMergeTree` unchanged — EXPLAIN
-- traversal yields `[a, b, c]`. With DP active the order is whatever DP's cost model picks (the
-- exact shape varies under the stateless randomizer); whatever it is, it will not be the original
-- `[a, b, c]`. So the test asserts the EXPLAIN read-step order is non-identity.
SELECT groupArray(extract(explain, '(pr_dp_a|pr_dp_b|pr_dp_c)')) != ['pr_dp_a', 'pr_dp_b', 'pr_dp_c']
FROM ( EXPLAIN PLAN
    SELECT count() FROM pr_dp_a AS a JOIN pr_dp_b AS b ON a.x = b.x JOIN pr_dp_c AS c ON b.x = c.x
    SETTINGS
        enable_parallel_replicas = 1,
        cluster_for_parallel_replicas = 'parallel_replicas',
        max_parallel_replicas = 3,
        parallel_replicas_for_non_replicated_merge_tree = 1,
        parallel_replicas_local_plan = 1,
        query_plan_join_swap_table = 'auto',
        query_plan_optimize_join_order_algorithm = 'dpsize',
        query_plan_optimize_join_order_limit = 10,
        query_plan_optimize_join_order_randomize = 0
)
WHERE explain LIKE '%ReadFromMergeTree%';

DROP TABLE pr_dp_a;
DROP TABLE pr_dp_b;
DROP TABLE pr_dp_c;
