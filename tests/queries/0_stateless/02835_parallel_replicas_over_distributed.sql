-- 1 shard

SELECT '-- 1 shard, 3 replicas';
DROP TABLE IF EXISTS test_d;
DROP TABLE IF EXISTS test;
CREATE TABLE test (id UInt64, date Date)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE IF NOT EXISTS test_d as test
ENGINE = Distributed(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test);

insert into test select *, today() from numbers(100);

SELECT count(), min(id), max(id), avg(id)
FROM test_d
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 3, prefer_localhost_replica = 0, parallel_replicas_for_non_replicated_merge_tree=1;

insert into test select *, today() from numbers(100);

SELECT count(), min(id), max(id), avg(id)
FROM test_d
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 3, prefer_localhost_replica = 0, parallel_replicas_for_non_replicated_merge_tree=1;

-- 2 shards

SELECT '-- 2 shards, 3 replicas each';
DROP TABLE IF EXISTS test2_d;
DROP TABLE IF EXISTS test2;
CREATE TABLE test2 (id UInt64, date Date)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE IF NOT EXISTS test2_d as test2
ENGINE = Distributed(test_cluster_two_shard_three_replicas_localhost, currentDatabase(), test2, id);

insert into test2 select *, today() from numbers(100);

SELECT count(), min(id), max(id), avg(id)
FROM test2_d
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 3, prefer_localhost_replica = 0, parallel_replicas_for_non_replicated_merge_tree=1;

insert into test2 select *, today() from numbers(100);

SELECT count(), min(id), max(id), avg(id)
FROM test2_d
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 3, prefer_localhost_replica = 0, parallel_replicas_for_non_replicated_merge_tree=1;
