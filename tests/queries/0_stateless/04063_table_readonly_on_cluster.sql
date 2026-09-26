-- Tags: no-replicated-database
-- Verify that OPTIMIZE TABLE ON CLUSTER does not hang when `table_readonly` is enabled.
-- The setting throws TABLE_IS_PERMANENTLY_READ_ONLY, which is non-retriable in DDLWorker,
-- while TABLE_IS_READ_ONLY remains retriable for transient ReplicatedMergeTree ZK disconnects.

DROP TABLE IF EXISTS t_readonly_cluster ON CLUSTER test_shard_localhost SYNC FORMAT Null;

CREATE TABLE t_readonly_cluster ON CLUSTER test_shard_localhost (x UInt64) ENGINE = MergeTree ORDER BY x FORMAT Null;
INSERT INTO t_readonly_cluster VALUES (1);

ALTER TABLE t_readonly_cluster ON CLUSTER test_shard_localhost MODIFY SETTING table_readonly = 1 FORMAT Null;

OPTIMIZE TABLE t_readonly_cluster ON CLUSTER test_shard_localhost FORMAT Null SETTINGS distributed_ddl_output_mode='throw'; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }

ALTER TABLE t_readonly_cluster ON CLUSTER test_shard_localhost MODIFY SETTING table_readonly = 0 FORMAT Null;

SELECT count() FROM t_readonly_cluster;

DROP TABLE t_readonly_cluster ON CLUSTER test_shard_localhost SYNC FORMAT Null;
