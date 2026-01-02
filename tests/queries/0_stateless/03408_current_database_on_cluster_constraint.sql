-- Tags: replica, no-parallel

DROP DATABASE IF EXISTS shard_0;

CREATE DATABASE shard_0;

SET distributed_ddl_entry_format_version = 2;
SET distributed_ddl_output_mode='throw';

CREATE TABLE shard_0.t0 ON CLUSTER 'test_cluster_two_shards_different_databases' (c0 Int, CONSTRAINT cc CHECK currentDatabase()) ENGINE = MergeTree() ORDER BY tuple();

DROP DATABASE shard_0;
