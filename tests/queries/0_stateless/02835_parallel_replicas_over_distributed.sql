DROP TABLE IF EXISTS test_d;
DROP TABLE IF EXISTS test;
CREATE TABLE test (id UInt64, date Date)
ENGINE = MergeTree
ORDER BY id
as select *, today() from numbers(100);

CREATE TABLE IF NOT EXISTS test_d as test
ENGINE = Distributed(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test);

SELECT count(), sum(id) 
FROM test_d
SETTINGS allow_experimental_parallel_reading_from_replicas = 1, max_parallel_replicas = 3, prefer_localhost_replica = 0, parallel_replicas_for_non_replicated_merge_tree=1, use_hedged_requests=0;
