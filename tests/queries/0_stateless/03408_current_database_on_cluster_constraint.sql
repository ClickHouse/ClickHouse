-- Tags: no-replicated-database

CREATE TABLE t0 ON CLUSTER 'test_shard_localhost' (c0 Int, CONSTRAINT cc CHECK currentDatabase()) ENGINE = MergeTree() ORDER BY tuple();
