-- Tags: no-replicated-database
-- Tag no-replicated-database: uses ON CLUSTER-style parallel replicas over an explicit cluster

-- `default_cluster` is used as the cluster for parallel replicas when `cluster_for_parallel_replicas` is not set.

DROP TABLE IF EXISTS t_default_cluster_pr;
CREATE TABLE t_default_cluster_pr (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_default_cluster_pr SELECT number FROM numbers(100);

SET enable_parallel_replicas = 2, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer

-- No cluster configured at all: neither `cluster_for_parallel_replicas` nor `default_cluster`.
SET cluster_for_parallel_replicas = '', default_cluster = '';
SELECT count() FROM t_default_cluster_pr WHERE NOT ignore(*); -- { serverError CLUSTER_DOESNT_EXIST }

-- `default_cluster` provides the cluster when `cluster_for_parallel_replicas` is empty.
SET cluster_for_parallel_replicas = '', default_cluster = 'test_cluster_one_shard_three_replicas_localhost';
SELECT count() FROM t_default_cluster_pr WHERE NOT ignore(*);

DROP TABLE t_default_cluster_pr;
