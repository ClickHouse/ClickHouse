-- Tags: shard
-- Test that cluster() table function accepts sharding key when a table function is passed as the 2nd argument.

SELECT * FROM cluster(test_shard_localhost, view(SELECT 1 AS x), x);
SELECT * FROM cluster(test_shard_localhost, cluster(test_shard_localhost, system.one), dummy);
SELECT * FROM clusterAllReplicas(test_shard_localhost, view(SELECT 1 AS x), x);
