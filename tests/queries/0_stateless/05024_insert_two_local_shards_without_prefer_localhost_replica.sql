-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-async-insert
-- Tag no-replicated-database: Fails due to additional replicas or shards
-- Tag no-shared-merge-tree: No quorum
-- Tag no-async-insert: async inserts are not supported with non-parallel quorum inserts

-- With `prefer_localhost_replica = 0` a local shard is normally written over TCP like a remote one,
-- and each such write is an independent query with its own `Too many parts` gate. When this server
-- is local for two destination shards, those sibling writes converge on the same local table, so one
-- of them could count the parts its sibling had just committed and fail with TOO_MANY_PARTS, or race
-- a non-parallel quorum insert. The writes of one query into the same local table must share its
-- gates, which is only possible in-process, so in this topology the local shards are written through
-- a nested in-process `INSERT` even with `prefer_localhost_replica = 0`.

DROP TABLE IF EXISTS t_05024_source;
DROP TABLE IF EXISTS t_05024_source_dist;
DROP TABLE IF EXISTS t_05024_target;
DROP TABLE IF EXISTS t_05024_target_dist;

CREATE TABLE t_05024_source (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_05024_source SELECT number FROM numbers(4);

CREATE TABLE t_05024_source_dist (n UInt64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_05024_source);

CREATE TABLE t_05024_target (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1;

CREATE TABLE t_05024_target_dist (n UInt64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_05024_target, n);

-- The `parallel_distributed_insert_select` rewrite runs one nested INSERT per destination shard.
-- Both shards are local, so both write into `t_05024_target`, and each shard reads its own local
-- source shard, hence 4 rows twice. With `max_insert_threads = 1` the two nested pipelines run one
-- after the other, so the second one starts writing when the part of the first one is already
-- committed - the case the shared gates fix.
INSERT INTO t_05024_target_dist SELECT * FROM t_05024_source_dist
    SETTINGS parallel_distributed_insert_select = 2, prefer_localhost_replica = 0,
        max_insert_threads = 1;

SELECT count() FROM t_05024_target;

TRUNCATE TABLE t_05024_target;

-- The regular foreground fan-out splits every block by the sharding key and runs one writing job
-- per destination shard. Both jobs write a part into `t_05024_target`, and they must share the
-- gates of the outer query for the same reason.
INSERT INTO t_05024_target_dist SELECT number FROM numbers(4)
    SETTINGS distributed_foreground_insert = 1, prefer_localhost_replica = 0;

SELECT count() FROM t_05024_target;

DROP TABLE t_05024_target_dist;
DROP TABLE t_05024_target;

-- The same topology for a non-parallel quorum insert: only one in-flight quorum part per table is
-- allowed, and the sibling writes of one query must not race two of them. In-process local jobs
-- are serialized by the sink; routed over TCP they would be two concurrent independent quorum
-- inserts into the same table.
CREATE TABLE t_05024_target_r1 (n UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_05024/target', 'r1') ORDER BY n;
CREATE TABLE t_05024_target_r2 (n UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_05024/target', 'r2') ORDER BY n;

CREATE TABLE t_05024_target_dist (n UInt64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_05024_target_r1, n);

INSERT INTO t_05024_target_dist SELECT number FROM numbers(4)
    SETTINGS distributed_foreground_insert = 1, prefer_localhost_replica = 0,
        insert_quorum = 2, insert_quorum_parallel = 0;

SELECT count() FROM t_05024_target_r1;

DROP TABLE t_05024_target_dist;
DROP TABLE t_05024_target_r2;
DROP TABLE t_05024_target_r1;
DROP TABLE t_05024_source_dist;
DROP TABLE t_05024_source;
