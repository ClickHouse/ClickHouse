-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-async-insert
-- Tag no-replicated-database: Fails due to additional replicas or shards
-- Tag no-shared-merge-tree: No quorum
-- Tag no-async-insert: async inserts are not supported with non-parallel quorum inserts

-- `parallel_distributed_insert_select` rewrites an `INSERT ... SELECT` between two `Distributed`
-- tables into one nested `INSERT` per destination shard, and with `prefer_localhost_replica` the
-- shards this server belongs to are written through a nested `INSERT` built right here. This server
-- can be local for several destination shards, and then those nested INSERTs all write into the
-- same underlying table concurrently. They must share the gates of the outer query, otherwise the
-- `Too many parts` check of a later one counts the parts its sibling has already committed: with
-- `parts_to_throw_insert = 1` the second nested INSERT failed with TOO_MANY_PARTS.

DROP TABLE IF EXISTS t_04847_source;
DROP TABLE IF EXISTS t_04847_source_dist;
DROP TABLE IF EXISTS t_04847_target;
DROP TABLE IF EXISTS t_04847_target_dist;

CREATE TABLE t_04847_source (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_04847_source SELECT number FROM numbers(4);

CREATE TABLE t_04847_source_dist (n UInt64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_04847_source);

CREATE TABLE t_04847_target (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1;

CREATE TABLE t_04847_target_dist (n UInt64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_04847_target);

-- Both shards of the destination cluster are local, so the query runs two nested INSERTs into
-- `t_04847_target`. Every shard of `parallel_distributed_insert_select = 2` writes the data of its
-- own local source shard, hence 4 rows twice.
-- With `max_insert_threads = 1` the two nested pipelines run one after the other, so the second one
-- starts writing when the part of the first one is already committed - the case the shared gates fix.
INSERT INTO t_04847_target_dist SELECT * FROM t_04847_source_dist
    SETTINGS parallel_distributed_insert_select = 2, prefer_localhost_replica = 1,
        max_insert_threads = 1;

SELECT count() FROM t_04847_target;

DROP TABLE t_04847_target_dist;
DROP TABLE t_04847_target;

-- The same topology for a non-parallel quorum insert: only one in-flight quorum part per table is
-- allowed, and the sibling nested INSERTs of one query must not race two of them. This half passes
-- without the fix as well - the nested pipelines of the local shards are not started concurrently,
-- so the quorum of one is resolved before the next one begins - and pins that property.
CREATE TABLE t_04847_target_r1 (n UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04847/target', 'r1') ORDER BY n;
CREATE TABLE t_04847_target_r2 (n UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04847/target', 'r2') ORDER BY n;

CREATE TABLE t_04847_target_dist (n UInt64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_04847_target_r1);

INSERT INTO t_04847_target_dist SELECT * FROM t_04847_source_dist
    SETTINGS parallel_distributed_insert_select = 2, prefer_localhost_replica = 1,
        insert_quorum = 2, insert_quorum_parallel = 0,
        max_insert_threads = 1;

SELECT count() FROM t_04847_target_r1;

DROP TABLE t_04847_target_dist;
DROP TABLE t_04847_target_r2;
DROP TABLE t_04847_target_r1;
DROP TABLE t_04847_source_dist;
DROP TABLE t_04847_source;
