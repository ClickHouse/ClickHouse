-- Companion of 04894_sorted_merge_join_parallel_replicas_runtime_filter for a join that ships inside the
-- parallel replicas fragment: both sides are replicated, so the whole join is distributed. The runtime
-- filter pass skips the join on the outer plan because `sorted_merge` is eligible there, but in the
-- fragment the coordinated input is no longer read in the order of the join keys and the selection falls
-- through to `hash`. The local plan of the fragment is optimized without the runtime filter pass (the
-- filters planted on the outer plan are already in it), so the skipped join must carry that decision
-- through the clone of the fragment and get its filter there. See PR #112973 review.

DROP TABLE IF EXISTS smjfrf_fact SYNC;
DROP TABLE IF EXISTS smjfrf_dim SYNC;

CREATE TABLE smjfrf_fact (id UInt64, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/smjfrf_fact', 'r1') ORDER BY id;
CREATE TABLE smjfrf_dim (id UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/smjfrf_dim', 'r1') ORDER BY id;
INSERT INTO smjfrf_fact SELECT number % 3000, number FROM numbers(4000);
INSERT INTO smjfrf_dim SELECT number % 2000, number * 2 FROM numbers(5000);

SET enable_analyzer = 1;

-- Pin the settings randomized in CI that the plan shape depends on.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;
-- The filter is listed as `Runtime filters:` of the read only when it is moved to `PREWHERE`.
SET optimize_move_to_prewhere = 1;

SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 0;

-- The local share of the fragment is expanded into the outer plan; its probe-side read lists the filter.
SELECT 'fragment_hash_has_runtime_filter', countIf(explain LIKE '%Runtime filters: RF%') = 1
FROM (EXPLAIN SELECT f.x FROM smjfrf_fact AS f INNER JOIN smjfrf_dim AS d ON f.id = d.id
      SETTINGS join_algorithm = 'sorted_merge,hash', max_threads = 4);

-- ...matching what plain `hash` gets under the same distributed plan.
SELECT 'fragment_plain_hash_has_runtime_filter', countIf(explain LIKE '%Runtime filters: RF%') = 1
FROM (EXPLAIN SELECT f.x FROM smjfrf_fact AS f INNER JOIN smjfrf_dim AS d ON f.id = d.id
      SETTINGS join_algorithm = 'hash', max_threads = 4);

SELECT count(), sum(f.x + d.v) FROM smjfrf_fact AS f INNER JOIN smjfrf_dim AS d ON f.id = d.id
SETTINGS join_algorithm = 'sorted_merge,hash';
SELECT count(), sum(f.x + d.v) FROM smjfrf_fact AS f INNER JOIN smjfrf_dim AS d ON f.id = d.id
SETTINGS join_algorithm = 'hash';

DROP TABLE smjfrf_fact SYNC;
DROP TABLE smjfrf_dim SYNC;
