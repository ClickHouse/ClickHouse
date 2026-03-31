-- Tags: no-parallel

-- Regression test for: LOGICAL_ERROR 'Shard number is greater than shard count' when
-- `cluster_for_parallel_replicas` has fewer shards than the outer Distributed cluster.
--
-- Trigger scenario (requires mixed server configs, not reproducible on a single server):
--   1. Outer Distributed table over cluster X (N shards, e.g. shard_num=2).
--   2. `cluster_for_parallel_replicas` = cluster Y (1 shard, multiple replicas).
--   3. `_shard_num=2` is set unconditionally by `ReadFromRemote::addPipe` for `shardNum()`.
--   4. `cluster_for_parallel_replicas` is NOT overridden to cluster X on the initiator
--      (e.g. because `allow_experimental_analyzer` differs between initiator and remote).
--   5. On the remote shard, `parallel_replicas` triggers for the local MergeTree.
--   6. `prepareClusterForParallelReplicas` reads `_shard_num=2` but `shard_count=1`
--      → LOGICAL_ERROR (server abort) before this fix.
--
-- After fix: stale `_shard_num > shard_count` with single-shard PR cluster emits WARNING
-- and uses all replicas. With multi-shard PR cluster it raises UNEXPECTED_CLUSTER
-- (same as shard_num=0 + multi-shard case).

SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer
SET serialize_query_plan = 0;
SET enable_parallel_replicas = 2;
SET max_parallel_replicas = 3;
SET parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (n UInt64) ENGINE = MergeTree() ORDER BY n;
INSERT INTO t SELECT * FROM numbers(10);

DROP TABLE IF EXISTS t_dist;
-- Outer cluster has 2 shards (both localhost:9000).
CREATE TABLE t_dist AS t ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), 't');

-- Main scenario: outer Distributed cluster has 2 shards, PR cluster has 1 shard (3 replicas).
-- In the single-server test the override path fires and no crash occurs regardless.
-- In production (mixed analyzer settings) this setup produced LOGICAL_ERROR before the fix.
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SELECT count() FROM t_dist;

-- Regression: querying a local table when cluster_for_parallel_replicas refers to a
-- multi-shard cluster must still raise UNEXPECTED_CLUSTER (not crash the server).
-- Use WHERE NOT ignore(*) to prevent trivial count optimization and ensure PR code path runs.
SET cluster_for_parallel_replicas = 'test_cluster_two_shards_localhost';
SELECT count() FROM t WHERE NOT ignore(*); -- { serverError UNEXPECTED_CLUSTER }

DROP TABLE t_dist;
DROP TABLE t;
