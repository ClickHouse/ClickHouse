-- With parallel replicas, a two-way ANY RIGHT JOIN under any_join_distinct_right_table_keys = 1 must return
-- the same rows as without them. Evaluated on each replica, every replica picked a right row for each left row
-- from its own share of the right table, so a left row was emitted once per replica.

SET automatic_parallel_replicas_mode = 0;
SET any_join_distinct_right_table_keys = 1;
SET explain_query_plan_default = 'legacy';
-- A randomized join order or a table swap would change the plan shape asserted below.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;

CREATE TABLE t_l (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_r (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 64;

INSERT INTO t_l SELECT number FROM numbers(10);
INSERT INTO t_r SELECT number % 10, number FROM numbers(100000);

-- Each left row takes one right row of its key, and every key is matched.
SELECT count() FROM t_l ANY RIGHT JOIN t_r ON t_l.k = t_r.k SETTINGS enable_parallel_replicas = 0;

-- The join is not shipped to the replicas.
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') > 0
FROM (
    EXPLAIN SELECT count() FROM t_l ANY RIGHT JOIN t_r ON t_l.k = t_r.k
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
        parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0
);

-- Positive control: ANY RIGHT JOIN without the setting still reads with parallel replicas.
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') > 0
FROM (
    EXPLAIN SELECT count() FROM t_l ANY RIGHT JOIN t_r ON t_l.k = t_r.k
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
        parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0, any_join_distinct_right_table_keys = 0
);

-- Plan-based parallel replicas: the join stays above the distributed read of t_r. The local plan is pinned
-- because it adds the initiator's own read to the plan shape.
SELECT arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (
        EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
        SELECT count() FROM t_l ANY RIGHT JOIN t_r ON t_l.k = t_r.k
        SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
            parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0, parallel_replicas_plan_based = 1,
            parallel_replicas_local_plan = 1
    )
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

-- Positive control: without the setting the whole join still ships.
SELECT arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (
        EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
        SELECT count() FROM t_l ANY RIGHT JOIN t_r ON t_l.k = t_r.k
        SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
            parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0, parallel_replicas_plan_based = 1,
            parallel_replicas_local_plan = 1, any_join_distinct_right_table_keys = 0
    )
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

DROP TABLE t_l;
DROP TABLE t_r;
