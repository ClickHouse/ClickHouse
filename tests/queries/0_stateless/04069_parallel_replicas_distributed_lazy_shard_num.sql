-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel
-- Tag no-parallel: failpoint use_delayed_remote_source is global and can force
-- DelayedSource on concurrent tests, causing "Unexpected lazy remote read from
-- a non-replicated table" crashes (same pattern as 02863).

-- Regression test for STID 5066: LOGICAL_ERROR "Shard number is greater than shard count"
-- when using parallel replicas over a distributed table with the use_delayed_remote_source failpoint.
--
-- Root cause: ReadFromRemote::addLazyPipe() did not override cluster_for_parallel_replicas
-- to match the distributed table's cluster (unlike addPipe()). The _shard_num scalar from
-- the distributed execution (e.g., shard_num=2) mismatched the parallel replicas cluster
-- (which has only 1 shard), causing a crash in prepareClusterForParallelReplicas.

SET allow_experimental_parallel_reading_from_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'parallel_replicas';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0;

DROP TABLE IF EXISTS t_04069_local;
DROP TABLE IF EXISTS t_04069_dist;

CREATE TABLE t_04069_local (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_04069_local SELECT number FROM numbers(1000);

-- Distributed table over a 2-shard cluster (both shards on 127.0.0.1, works in all CI environments)
CREATE TABLE t_04069_dist AS t_04069_local
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_04069_local);

-- Enable the failpoint that forces the lazy remote source path
SYSTEM ENABLE FAILPOINT use_delayed_remote_source;

-- This query previously crashed with:
-- LOGICAL_ERROR: Shard number is greater than shard count: shard_num=2 shard_count=1 cluster=parallel_replicas
-- Use FORMAT Null because the exact result depends on whether Keeper is available for parallel replicas coordination.
-- Without Keeper, parallel replicas falls back to normal distributed execution (2 shards → double sum).
-- The test's purpose is verifying no crash, not checking the exact value.
SELECT sum(a) FROM t_04069_dist FORMAT Null;
SELECT 'OK';

SYSTEM DISABLE FAILPOINT use_delayed_remote_source;

DROP TABLE t_04069_dist;
DROP TABLE t_04069_local;
