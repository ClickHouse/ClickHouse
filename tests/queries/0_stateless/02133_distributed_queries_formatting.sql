SELECT * FROM cluster(test_cluster_two_shards, view(SELECT 'Hello' AS all, 'World' AS distinct));
