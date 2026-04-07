-- Tags: zookeeper, no-parallel

DROP TABLE IF EXISTS t_hardware_error NO DELAY;

CREATE TABLE t_hardware_error (
	KeyID UInt32
) Engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/t_async_insert_dedup', '{replica}')
ORDER BY (KeyID);

insert into t_hardware_error values (1), (2), (3), (4), (5); 

system enable failpoint replicated_merge_tree_commit_zk_fail_after_op;

insert into t_hardware_error values (6), (7), (8), (9), (10); 

select count() from t_hardware_error;

system disable failpoint replicated_commit_zk_fail_after_op;

DROP TABLE t_hardware_error NO DELAY;
