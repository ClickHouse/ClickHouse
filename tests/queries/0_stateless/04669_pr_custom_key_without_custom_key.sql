-- `parallel_replicas_mode` asks for the custom key filtering, but `parallel_replicas_custom_key` is
-- not set, so there is no way to give every replica its own part of the data to read.
--
-- Every replica reads only what its filter selects, so without the key every replica reads everything
-- and the result of the shard is multiplied by the number of the replicas. The query has to fail
-- instead, no matter how many shards the cluster has and which side builds the filter: the initiator
-- builds it for a cluster with a single shard, and the replicas build it themselves otherwise.
--
-- The table is a plain `MergeTree`, so the queries enable `parallel_replicas_for_non_replicated_merge_tree`,
-- otherwise the parallel replicas are not used for it at all and there is nothing to report.

DROP TABLE IF EXISTS d_two_shards;
DROP TABLE IF EXISTS d_single_shard;
DROP TABLE IF EXISTS t;

CREATE TABLE t (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t SELECT number, number FROM numbers(1000);

CREATE TABLE d_two_shards AS t ENGINE = Distributed('test_cluster_two_shard_three_replicas_localhost', currentDatabase(), t);
CREATE TABLE d_single_shard AS t ENGINE = Distributed('test_cluster_one_shard_three_replicas_localhost', currentDatabase(), t);

SELECT count(), sum(y) FROM d_two_shards
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, prefer_localhost_replica = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

SELECT count(), sum(y) FROM d_two_shards
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, prefer_localhost_replica = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_mode = 'custom_key_range'; -- { serverError BAD_ARGUMENTS }

SELECT count(), sum(y) FROM d_single_shard
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, prefer_localhost_replica = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

-- With the custom key the replicas read different parts of the data. The first cluster has 2 shards.
SELECT count(), sum(y) FROM d_two_shards
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, prefer_localhost_replica = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'x';

DROP TABLE d_single_shard;
DROP TABLE d_two_shards;
DROP TABLE t;
