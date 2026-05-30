-- Regression test: the DP join-order optimizer must keep running when parallel replicas is enabled.
-- Previously `QueryPlanOptimizationSettings` forced `query_plan_optimize_join_order_limit=0`
-- whenever `allow_experimental_parallel_reading_from_replicas && max_parallel_replicas > 1`, which
-- silently skipped join reordering for any PR query.

-- `query_plan_optimize_join_order_randomize` injects random cardinalities/NDVs into the DP cost
-- model when non-zero, so the chosen tree shape becomes non-deterministic. Pin it off at the
-- session level so the EXPLAIN output is stable under any randomizer config.
SET query_plan_optimize_join_order_randomize = 0;

-- For runs with old analyzer
SET enable_analyzer=1;

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
  -- Transitive predicate derivation adds the implicit `a.x = c.x` edge to the join graph,
  -- which gives DP a different cost surface and a different left-deep choice. Pin it off so
  -- the EXPLAIN order is stable regardless of the randomizer.
  enable_join_transitive_predicates = 0,
  automatic_parallel_replicas_mode = 0;

-- Three-way join written as `(a JOIN b) JOIN c` with `a` (100 rows) < `b` (1000) < `c` (100000).
-- The 2-table swap can also go through the legacy `optimizeJoinLegacy` path, so this test forces
-- `query_plan_optimize_join_order_algorithm='dpsize'` and uses a 3-way join to exercise the actual
-- DP optimizer (`query_graph_size_limit > 2` in `optimizeJoinLogicalImpl`).
--
-- These tables have no column statistics, so every join-key NDV is unknown and the DP
-- cardinality estimates are untrusted (see #101398). With `query_plan_join_swap_table='auto'`
-- the size-based build-side swap is suppressed for untrusted sides, so DP still reorders the
-- inner pair to `(b JOIN a)` but does not move the large `c` to the probe side on the strength
-- of a fabricated estimate — building the hash table from the known-bounded `c` rather than the
-- unknown-size intermediate. The EXPLAIN read-step traversal is therefore `b, a, c`. With DP
-- disabled (`query_plan_optimize_join_order_limit=0` — the previously forced behaviour)
-- `optimizeJoinLogicalImpl` exits early without converting the `JoinStepLogical`s, and
-- `optimizeJoinLegacy` is a no-op on them, so the user-written order `(a JOIN b) JOIN c` reaches
-- `ReadFromMergeTree` unchanged and the traversal is `a, b, c`.
SELECT extract(explain, '(pr_dp_a|pr_dp_b|pr_dp_c)')
FROM (
  EXPLAIN PLAN
  SELECT count() FROM pr_dp_a AS a JOIN pr_dp_b AS b ON a.x = b.x JOIN pr_dp_c AS c ON b.x = c.x
)
WHERE explain LIKE '%ReadFromMergeTree%';

-- Check the query result itself
SELECT count() FROM pr_dp_a AS a JOIN pr_dp_b AS b ON a.x = b.x JOIN pr_dp_c AS c ON b.x = c.x;

DROP TABLE pr_dp_a;
DROP TABLE pr_dp_b;
DROP TABLE pr_dp_c;
