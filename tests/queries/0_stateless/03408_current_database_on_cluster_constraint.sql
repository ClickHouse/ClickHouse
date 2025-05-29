
CREATE TABLE t0 ON CLUSTER 'test_cluster_two_shards_localhost' (c0 Int, CONSTRAINT cc CHECK currentDatabase()) ENGINE = MergeTree() ORDER BY tuple();
