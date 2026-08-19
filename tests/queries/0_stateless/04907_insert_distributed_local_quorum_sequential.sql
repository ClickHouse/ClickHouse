-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-async-insert
-- Tag no-replicated-database: Fails due to additional replicas or shards
-- Tag no-shared-merge-tree: No quorum
-- Tag no-async-insert: async inserts are not supported with non-parallel quorum inserts

-- A synchronous foreground INSERT into a Distributed table starts one nested local INSERT per
-- local shard. If two local shards point at the same ReplicatedMergeTree table, they must be
-- started sequentially for a non-parallel quorum insert: two in-flight quorum parts for that
-- table conflict on its quorum status node.

DROP TABLE IF EXISTS t_04907_dist;
DROP TABLE IF EXISTS t_04907_target_r2;
DROP TABLE IF EXISTS t_04907_target_r1;

CREATE TABLE t_04907_target_r1 (n UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04907/target', 'r1') ORDER BY n;
CREATE TABLE t_04907_target_r2 (n UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04907/target', 'r2') ORDER BY n;

CREATE TABLE t_04907_dist (n UInt64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_04907_target_r1, n);

-- `0` and `1` belong to different shards, so both local jobs write a quorum part into
-- `t_04907_target_r1`. Keep the outer INSERT split into two blocks: on the second block,
-- `ReplicatedMergeTreeSink::consume` commits the first block before `onFinish` runs.
INSERT INTO t_04907_dist SELECT number FROM numbers(2)
    SETTINGS distributed_foreground_insert = 1, prefer_localhost_replica = 1,
        insert_quorum = 2, insert_quorum_parallel = 0, max_distributed_connections = 2,
        max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1;

SELECT count(), sum(n) FROM t_04907_target_r1 SETTINGS select_sequential_consistency = 1;

DROP TABLE t_04907_dist;
DROP TABLE t_04907_target_r2;
DROP TABLE t_04907_target_r1;
