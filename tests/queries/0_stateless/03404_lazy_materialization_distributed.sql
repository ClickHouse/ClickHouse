DROP TABLE IF EXISTS tt;
CREATE TABLE tt (k UInt64, v String, blob String) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO tt SELECT number, toString(number), repeat('blob_bob', number) FROM numbers(1, 10);

-- make sure the optimization is enabled
set query_plan_optimize_lazy_materialization=true, query_plan_max_limit_for_lazy_materialization=10;

SELECT
    v,
    blob
FROM clusterAllReplicas(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), tt)
ORDER BY k
LIMIT 3;

DROP TABLE tt;
