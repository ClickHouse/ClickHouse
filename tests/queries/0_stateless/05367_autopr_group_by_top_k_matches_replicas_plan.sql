-- Automatic parallel replicas runs after the `GROUP BY` top-K heap is placed, and parallel replicas use the heap too, so
-- the single-node plan and the plan with parallel replicas agree on a `GROUP BY ... LIMIT` aggregation and it can be matched.
-- Red if the boundary is not matched: then no statistics are collected and `RuntimeDataflowStatisticsOutputBytes` stays 0.

DROP TABLE IF EXISTS t_group_by_top_k;

CREATE TABLE t_group_by_top_k (k UInt64, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO t_group_by_top_k SELECT number % 100000, toString(number % 7) FROM numbers(1000000);

SET enable_analyzer = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1;
-- The heap refuses `max_rows_to_group_by`, which the test profile sets.
-- Without the heap both queries below share one boundary, and the second decides from the statistics of the first.
SET enable_group_by_top_k_optimization = 1, max_rows_to_group_by = 0, query_plan_max_limit_for_top_k_optimization = 1000;
-- The per-replica floor skips building the plan on its own.
SET automatic_parallel_replicas_min_bytes_per_replica = 0;

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1;
SELECT k, s, count() FROM t_group_by_top_k GROUP BY k, s LIMIT 3 FORMAT Null SETTINGS log_comment = 'top_k_limit';
SELECT k, s, count() FROM t_group_by_top_k GROUP BY k, s ORDER BY k LIMIT 3 FORMAT Null SETTINGS log_comment = 'top_k_order_by';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('top_k_limit', 'top_k_order_by')
ORDER BY log_comment;

-- With parallel replicas, `GROUP BY ... LIMIT` returns the smallest groups, and each replica keeps only those in its heap.
SET automatic_parallel_replicas_mode = 0;
SELECT k, count() FROM t_group_by_top_k GROUP BY k LIMIT 3 OFFSET 2;
SELECT replaceRegexpAll(explain, '^[│└├─ ]+', '') FROM (EXPLAIN actions = 1 SELECT k, count() FROM t_group_by_top_k GROUP BY k LIMIT 3)
WHERE explain LIKE '%Top-K%' OR explain LIKE '%Sorting (%';
-- Above the limit the heap refuses, the query is not ordered: that would only cost a sort.
SELECT count() FROM (EXPLAIN SELECT k, count() FROM t_group_by_top_k GROUP BY k LIMIT 1001) WHERE explain LIKE '%Sorting (%';

DROP TABLE t_group_by_top_k;
