-- `tryAddJoinRuntimeFilter` skips a join when an eligible `sorted_merge` wins the selection (a merge
-- join cannot use a runtime filter). When `applyParallelReplicas` later rewrites the coordinated side
-- into a distributed read, the eligibility is recomputed and the selection falls through to `hash` -
-- and the suppressed runtime filter must be re-added for that hash join, or `sorted_merge,hash` would
-- silently run slower than plain `hash` on this edge. Companion of
-- 04824_sorted_merge_join_parallel_replicas_fallthrough, which pins the algorithm fall-through itself.
-- See PR #112973 review.

DROP TABLE IF EXISTS smjrf_fact SYNC;
DROP TABLE IF EXISTS smjrf_dim SYNC;

-- The join key is the primary key of both sides, so without parallel replicas `sorted_merge` is eligible.
CREATE TABLE smjrf_fact (id UInt64, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/smjrf_fact', 'r1') ORDER BY id;
CREATE TABLE smjrf_dim (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO smjrf_fact SELECT number % 3000, number FROM numbers(4000);
INSERT INTO smjrf_dim SELECT number % 2000, number * 2 FROM numbers(5000);

-- The eligibility of `sorted_merge` is decided on the query plan, which exists only for the analyzer.
SET enable_analyzer = 1;

-- Pin the settings randomized in CI that the plan shape depends on.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;

-- Sanity: with local reads `sorted_merge` wins the selection, so the runtime filter is suppressed.
SELECT 'local_no_runtime_filter', countIf(explain LIKE '%BuildRuntimeFilter%') = 0
FROM (EXPLAIN SELECT f.x FROM smjrf_fact AS f INNER JOIN smjrf_dim AS d ON f.id = d.id
      SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4, enable_parallel_replicas = 0);

-- The suppression must not change what plain `hash` gets: the filter is planted there.
SELECT 'local_hash_has_runtime_filter', countIf(explain LIKE '%BuildRuntimeFilter%') = 1
FROM (EXPLAIN SELECT f.x FROM smjrf_fact AS f INNER JOIN smjrf_dim AS d ON f.id = d.id
      SETTINGS join_algorithm = 'hash', max_threads = 4, enable_parallel_replicas = 0);

-- An earlier `full_sorting_merge` is always selected and makes the later `sorted_merge` unreachable.
-- It must not suppress the runtime filter merely because the ordered inputs would otherwise make
-- `sorted_merge` eligible.
SELECT 'full_sorting_merge_before_sorted_merge_has_runtime_filter', countIf(explain LIKE '%BuildRuntimeFilter%') = 1
FROM (EXPLAIN SELECT f.x FROM smjrf_fact AS f INNER JOIN smjrf_dim AS d ON f.id = d.id
      SETTINGS join_algorithm = 'full_sorting_merge,sorted_merge,hash', max_threads = 4, enable_parallel_replicas = 0);

SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 0;

-- The scenario of 04824: the broadcast side (`smjrf_dim`) is unshippable, so the join stays in the
-- outer plan while the coordinated read of `smjrf_fact` becomes a distributed read below it. The
-- selection falls through to `hash` - and the runtime filter suppressed for `sorted_merge` on the
-- pre-rewrite plan must be back on the post-rewrite hash plan.
SELECT 'parallel_replicas_hash_has_runtime_filter', countIf(explain LIKE '%BuildRuntimeFilter%') = 1
FROM (EXPLAIN SELECT f.x FROM smjrf_fact AS f INNER JOIN smjrf_dim AS d ON f.id = d.id
      SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

-- ...matching what plain `hash` gets under the same distributed plan.
SELECT 'parallel_replicas_plain_hash_has_runtime_filter', countIf(explain LIKE '%BuildRuntimeFilter%') = 1
FROM (EXPLAIN SELECT f.x FROM smjrf_fact AS f INNER JOIN smjrf_dim AS d ON f.id = d.id
      SETTINGS join_algorithm = 'hash', max_threads = 4);

-- The filtered fall-through result matches a plain `hash` join under the same distributed plan.
SELECT count(), sum(f.x + d.v) FROM smjrf_fact AS f INNER JOIN smjrf_dim AS d ON f.id = d.id
SETTINGS join_algorithm = 'sorted_merge,hash';
SELECT count(), sum(f.x + d.v) FROM smjrf_fact AS f INNER JOIN smjrf_dim AS d ON f.id = d.id
SETTINGS join_algorithm = 'hash';

DROP TABLE smjrf_fact SYNC;
DROP TABLE smjrf_dim SYNC;
