-- The old analyzer switches parallel replicas off on the replica for a plain `MergeTree` table unless
-- `parallel_replicas_for_non_replicated_merge_tree` is set, and that also switched off the filtering by the
-- custom key. The initiator had already sent the query to every replica of the shard, so every replica read
-- the whole table and the result of the shard was multiplied by the number of the replicas.
--
-- That setting is about the task based parallel replicas, which read a part of the data assigned by the
-- coordinator. Reading a part of the data selected by the custom key does not depend on it, and the analyzer
-- does not disable the filtering here.

DROP TABLE IF EXISTS d;
DROP TABLE IF EXISTS t;

CREATE TABLE t (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t SELECT number, number FROM numbers(1000);

CREATE TABLE d AS t ENGINE = Distributed('test_cluster_two_shard_three_replicas_localhost', currentDatabase(), t);

-- The cluster has 2 shards, so the counts are twice the number of the rows.
SELECT count(), sum(y) FROM d
SETTINGS enable_analyzer = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3, prefer_localhost_replica = 0,
    parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'x';

SELECT count(), sum(y) FROM d
SETTINGS enable_analyzer = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3, prefer_localhost_replica = 0,
    parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'x';

-- Without the custom key there is nothing to filter by, and the query fails the same way it does with the analyzer.
SELECT count(), sum(y) FROM d
SETTINGS enable_analyzer = 0, enable_parallel_replicas = 1, max_parallel_replicas = 3, prefer_localhost_replica = 0,
    parallel_replicas_mode = 'custom_key_sampling'; -- { serverError BAD_ARGUMENTS }

DROP TABLE d;
DROP TABLE t;
