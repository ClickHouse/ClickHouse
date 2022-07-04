SET prefer_localhost_replica = 1;
SET optimize_monotonous_functions_in_order_by = 1;

SELECT *
FROM cluster(test_cluster_two_shards_localhost, system, one)
ORDER BY toDateTime(dummy);
