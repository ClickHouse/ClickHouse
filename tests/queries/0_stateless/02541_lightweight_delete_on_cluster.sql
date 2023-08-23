-- Tags: distributed, no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is not allowed

SET distributed_ddl_output_mode='throw';

CREATE TABLE t1_local ON CLUSTER test_shard_localhost(partition_col_1 String, tc1 int,tc2 int) ENGINE=MergeTree() PARTITION BY partition_col_1 ORDER BY tc1;

INSERT INTO t1_local VALUES('partition1', 1,1);
INSERT INTO t1_local VALUES('partition2', 1,2);
INSERT INTO t1_local VALUES('partition1', 2,3);
INSERT INTO t1_local VALUES('partition2', 2,4);

-- { echoOn }

SELECT * FROM t1_local ORDER BY tc1, tc2;

DELETE FROM t1_local ON CLUSTER test_shard_localhost WHERE tc1 = 1;

SELECT * FROM t1_local ORDER BY tc1, tc2;

-- { echoOff }
