DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (key UInt8) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=1024;
INSERT INTO t1 SELECT number % 1000  from numbers(100000);

CREATE TABLE t2 (key UInt64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=1024;
INSERT INTO t2 SELECT number % 1000 from numbers(100000);

SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree=1, parallel_replicas_filter_pushdown=1;

SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SYSTEM ENABLE FAILPOINT parallel_replicas_check_read_mode_always;

SELECT a FROM (SELECT key + 1 as a, key FROM t1 GROUP BY key HAVING key) settings parallel_replicas_local_plan=1, optimize_aggregation_in_order=1 FORMAT Null;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
