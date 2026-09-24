-- With `enable_parallel_replicas = 1` the runtime-filter pass runs while every join is still logical, so the order
-- a nested join emits is predicted from the `join_algorithm` list (`predictMergeJoinOutputOrder`). The prediction
-- must skip `partial_merge` where it declines the nested join, like the selection does: `MergeJoin` cannot run an
-- `ANY RIGHT` join. If the prediction stopped at it, the nested `sorted_merge` join would be taken as unordered, the join above it
-- would get a runtime filter, and planting the filter erases its merge algorithms: the chain of merge joins
-- would silently degrade to a hash join on top. See PR #112973 review.

DROP TABLE IF EXISTS pso_a;
DROP TABLE IF EXISTS pso_b;
DROP TABLE IF EXISTS pso_c;

CREATE TABLE pso_a (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pso_b (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE pso_c (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO pso_a SELECT number, number FROM numbers(20000);
INSERT INTO pso_b SELECT number % 15000, number FROM numbers(30000);
INSERT INTO pso_c SELECT number * 2, number FROM numbers(12000);

-- Pin the settings randomized in CI that the plan shape depends on. Without a spill threshold the hash joins are
-- not wrapped into a spilling one.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 0;
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, max_threads = 4;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0, explain_query_plan_default = 'legacy';

-- The tables are not replicated and `parallel_replicas_for_non_replicated_merge_tree = 0`, so the reads stay
-- local (and in order) and both joins can be merge joins, but the runtime-filter pass still runs on the logical
-- plan, as it does for parallel replicas.
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 0, parallel_replicas_plan_based = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 1;

-- Reference: with `sorted_merge` first, both joins are merge joins and no runtime filter is planted.
SELECT 'sorted_merge_first', countIf(explain LIKE '%MergeJoinTransform%') = 2, countIf(explain LIKE '%RuntimeFilter%') = 0
FROM (EXPLAIN PIPELINE SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM pso_a AS a ANY RIGHT JOIN pso_b AS b ON a.id = b.id ANY RIGHT JOIN pso_c AS c ON b.id = c.id
      SETTINGS join_algorithm = 'sorted_merge,hash');

-- `partial_merge` declines both `ANY RIGHT` joins, so the list behaves as `sorted_merge,hash`.
SELECT 'partial_merge_declines', countIf(explain LIKE '%MergeJoinTransform%') = 2, countIf(explain LIKE '%RuntimeFilter%') = 0
FROM (EXPLAIN PIPELINE SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM pso_a AS a ANY RIGHT JOIN pso_b AS b ON a.id = b.id ANY RIGHT JOIN pso_c AS c ON b.id = c.id
      SETTINGS join_algorithm = 'partial_merge,sorted_merge,hash');

-- Where `partial_merge` does run the nested join (`INNER ALL`), its output order is not exploitable: the join
-- above falls through to `hash` and gets its runtime filter.
SELECT 'partial_merge_selected', countIf(explain LIKE '%Algorithm: PartialMergeJoin%') = 1, countIf(explain LIKE '%Algorithm: HashJoin%') = 1, countIf(explain LIKE '%BuildRuntimeFilter%') = 1
FROM (EXPLAIN actions = 1 SELECT sum(a.v) + sum(b.v) + sum(c.v) FROM pso_a AS a INNER JOIN pso_b AS b ON a.id = b.id ANY RIGHT JOIN pso_c AS c ON b.id = c.id
      SETTINGS join_algorithm = 'partial_merge,sorted_merge,hash');

-- The results match a plain `hash` join.
SELECT 'partial_merge_declines_result',
    (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM pso_a AS a ANY RIGHT JOIN pso_b AS b ON a.id = b.id ANY RIGHT JOIN pso_c AS c ON b.id = c.id SETTINGS join_algorithm = 'partial_merge,sorted_merge,hash')
  = (SELECT (sum(a.v), sum(b.v), sum(c.v), count()) FROM pso_a AS a ANY RIGHT JOIN pso_b AS b ON a.id = b.id ANY RIGHT JOIN pso_c AS c ON b.id = c.id SETTINGS join_algorithm = 'hash');

DROP TABLE pso_a;
DROP TABLE pso_b;
DROP TABLE pso_c;
