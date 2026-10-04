-- The eligibility of `sorted_merge` / `parallel_sorted_merge` is memoized on the logical join step
-- before `applyParallelReplicas` runs. When the join itself cannot ship (here: the broadcast side is a
-- non-replicated `MergeTree`, unsafe to read on every replica), the join stays local while its
-- coordinated side is rewritten into a distributed read - which cannot be read in the order of the join
-- keys anymore. The memoized answer must not leak into physicalization: the selection has to fall
-- through to the next entry of `join_algorithm` (`hash`), not keep a merge join with full pre-join
-- sorts. See PR #112973 review.

DROP TABLE IF EXISTS smjpr_fact SYNC;
DROP TABLE IF EXISTS smjpr_dim SYNC;

-- The join key is the primary key of both sides, so without parallel replicas `sorted_merge` is eligible.
CREATE TABLE smjpr_fact (id UInt64, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/smjpr_fact', 'r1') ORDER BY id;
CREATE TABLE smjpr_dim (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO smjpr_fact SELECT number % 3000, number FROM numbers(4000);
INSERT INTO smjpr_dim SELECT number % 2000, number * 2 FROM numbers(5000);

-- The eligibility of `sorted_merge` is decided on the query plan, which exists only for the analyzer.
SET enable_analyzer = 1;

-- Pin the settings randomized in CI that the plan shape depends on: the in-order read must be allowed,
-- the PK-range sharding stays out of the picture, and the sides are not swapped or reordered.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;

-- Sanity: with local reads the primary-key join selects `sorted_merge` - a merge join runs.
SELECT 'local_sorted_merge_selected', countIf(explain LIKE '%MergeJoinTransform%') >= 1
FROM (EXPLAIN PIPELINE SELECT f.x FROM smjpr_fact AS f INNER JOIN smjpr_dim AS d ON f.id = d.id
      SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4, enable_parallel_replicas = 0);

SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 0;

-- The scenario: the broadcast side (`smjpr_dim`) is unshippable, so the join stays in the outer plan
-- while the coordinated read of `smjpr_fact` becomes a distributed read below it.
SELECT 'join_stays_local_with_distributed_read', arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT f.x FROM smjpr_fact AS f INNER JOIN smjpr_dim AS d ON f.id = d.id
          SETTINGS join_algorithm = 'sorted_merge,hash')
    WHERE step IN ('Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

-- The distributed read cannot be read in join-key order, so `sorted_merge` must not be selected: the
-- stale pre-rewrite eligibility must be recomputed and the selection falls through to `hash`.
SELECT 'parallel_replicas_falls_through_to_hash', countIf(explain LIKE '%MergeJoinTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT f.x FROM smjpr_fact AS f INNER JOIN smjpr_dim AS d ON f.id = d.id
      SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

-- ...and it must not pay for the merge join it did not run: no full sorts either.
SELECT 'parallel_replicas_no_full_sorts', countIf(explain LIKE '%MergeSortingTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT f.x FROM smjpr_fact AS f INNER JOIN smjpr_dim AS d ON f.id = d.id
      SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

-- The fall-through result matches a plain `hash` join under the same distributed plan.
SELECT count(), sum(f.x + d.v) FROM smjpr_fact AS f INNER JOIN smjpr_dim AS d ON f.id = d.id
SETTINGS join_algorithm = 'sorted_merge,hash';
SELECT count(), sum(f.x + d.v) FROM smjpr_fact AS f INNER JOIN smjpr_dim AS d ON f.id = d.id
SETTINGS join_algorithm = 'hash';

DROP TABLE smjpr_fact SYNC;
DROP TABLE smjpr_dim SYNC;
