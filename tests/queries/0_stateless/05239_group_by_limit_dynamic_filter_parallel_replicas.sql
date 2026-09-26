-- `enable_group_by_top_k_dynamic_filtering` is not applied to reads from parallel replicas: the heap
-- boundary is not carried to the remote reads. Check that `GROUP BY key [ORDER BY key] LIMIT n` still
-- returns the right groups there, with the plan shipped to the replicas both as a query plan and as text.

DROP TABLE IF EXISTS t_gb_dyn_pr;

CREATE TABLE t_gb_dyn_pr (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 128;
INSERT INTO t_gb_dyn_pr SELECT number % 1000, number FROM numbers(100000);

SET enable_group_by_top_k_optimization = 1;
SET enable_group_by_top_k_dynamic_filtering = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT 'plan';
SELECT a, count(), sum(b) FROM t_gb_dyn_pr GROUP BY a ORDER BY a LIMIT 3 SETTINGS serialize_query_plan = 1;
SELECT a, count(), sum(b) FROM t_gb_dyn_pr GROUP BY a ORDER BY a DESC LIMIT 3 SETTINGS serialize_query_plan = 1;

SELECT 'text';
SELECT a, count(), sum(b) FROM t_gb_dyn_pr GROUP BY a ORDER BY a LIMIT 3 SETTINGS serialize_query_plan = 0;
SELECT a, count(), sum(b) FROM t_gb_dyn_pr GROUP BY a ORDER BY a DESC LIMIT 3 SETTINGS serialize_query_plan = 0;

-- Without `ORDER BY` any 3 groups are a valid answer, but each must be complete.
SELECT 'no order by';
SELECT count(), min(c), max(c) FROM (SELECT a, count() AS c FROM t_gb_dyn_pr GROUP BY a LIMIT 3);

DROP TABLE t_gb_dyn_pr;
