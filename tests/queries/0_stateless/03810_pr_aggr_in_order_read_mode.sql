DROP TABLE IF EXISTS t1;

CREATE TABLE t1 (a UInt8) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity=1;
INSERT INTO t1 SELECT number % 100 from numbers(10000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree=1;
SET optimize_read_in_order=0;
SET optimize_aggregation_in_order=1; -- issue is related to this optimization

SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SYSTEM ENABLE FAILPOINT parallel_replicas_check_read_mode_always;

-- TODO: this query will fail if parallel_replicas_filter_pushdown is enabled
--       enable parallel_replicas_filter_pushdown setting explicitly
--       after https://github.com/ClickHouse/ClickHouse/issues/95524 is fixed
SELECT a
FROM t1
GROUP BY a
HAVING materialize(0)
SETTINGS parallel_replicas_local_plan = 1;

DROP TABLE t1;
