-- Tags: distributed, no-parallel, no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is not allowed

DROP DATABASE IF EXISTS 02028_db ON CLUSTER test_shard_localhost;
CREATE DATABASE 02028_db ON CLUSTER test_shard_localhost;
USE 02028_db;

CREATE TABLE t1_local ON CLUSTER test_shard_localhost(partition_col_1 String, tc1 int,tc2 int)ENGINE=MergeTree() PARTITION BY partition_col_1 ORDER BY tc1;
CREATE TABLE t2_local ON CLUSTER test_shard_localhost(partition_col_1 String, tc1 int,tc2 int)ENGINE=MergeTree() PARTITION BY partition_col_1 ORDER BY tc1;

INSERT INTO t1_local VALUES('partition1', 1,1);
INSERT INTO t1_local VALUES('partition2', 1,1);
INSERT INTO t2_local VALUES('partition1', 3,3);
INSERT INTO t2_local VALUES('partition2', 6,6);

ALTER TABLE t1_local ON CLUSTER test_shard_localhost REPLACE PARTITION 'partition1' FROM  t2_local;
ALTER TABLE t1_local ON CLUSTER test_shard_localhost MOVE PARTITION 'partition2' TO TABLE t2_local;

DROP DATABASE 02028_db ON CLUSTER test_shard_localhost;
