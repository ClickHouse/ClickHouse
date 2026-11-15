-- Tags: no-fasttest
-- s3Cluster is not used in fast tests

SELECT * FROM s3Cluster('test_cluster_multiple_nodes_all_unavailable', 'http://localhost:11111/test/a.tsv'); -- { serverError ALL_CONNECTION_TRIES_FAILED }
