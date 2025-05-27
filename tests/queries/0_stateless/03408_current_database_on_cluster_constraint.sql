-- Tags: no-replicated-database

CREATE TABLE t0 ON CLUSTER 'test_cluster_two_shards_different_databases' (c0 Int, CONSTRAINT cc CHECK currentDatabase()) ENGINE = MergeTree() ORDER BY tuple();
