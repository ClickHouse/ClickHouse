-- Tags: zookeeper, no-parallel, no-shared-merge-tree
-- no-shared-merge-tree: This failure injection is only RMT specific

DROP TABLE IF EXISTS t_hardware_error NO DELAY;

CREATE TABLE t_hardware_error (
    KeyID UInt32
) Engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/t_async_insert_dedup', '{replica}')
ORDER BY (KeyID);

insert into t_hardware_error values (1), (2), (3), (4), (5);

-- Data is written to ZK but the connection fails right after and we can't recover it
system enable failpoint replicated_merge_tree_commit_zk_fail_after_op;
system enable failpoint replicated_merge_tree_commit_zk_fail_when_recovering_from_hw_fault;

insert into t_hardware_error values (6), (7), (8), (9), (10); -- {serverError UNKNOWN_STATUS_OF_INSERT}

system disable failpoint replicated_commit_zk_fail_after_op;
system disable failpoint replicated_merge_tree_commit_zk_fail_when_recovering_from_hw_fault;

insert into t_hardware_error values (11), (12), (13), (14), (15);

-- All 3 commits have been written correctly. The unknown status is ok (since it failed after the operation)
Select arraySort(groupArray(KeyID)) FROM t_hardware_error;

DROP TABLE t_hardware_error NO DELAY;
