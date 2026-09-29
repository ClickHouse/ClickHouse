-- A replica executes the whole query unless it is given a part of the data to read, so there must be
-- a single connection per shard. Otherwise the result of the shard is multiplied by the number of the
-- connections. Parallel reading from replicas is not applicable here (`automatic_parallel_replicas_mode`
-- disables it), so every replica would return all the rows of the shard.

DROP TABLE IF EXISTS d;
DROP TABLE IF EXISTS t;

CREATE TABLE t (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t SELECT number, number FROM numbers(1000);

CREATE TABLE d AS t ENGINE = Distributed('test_cluster_two_shard_three_replicas_localhost', currentDatabase(), t);

-- The cluster has 2 shards, so the counts are twice the number of the rows.
SELECT count(), sum(y) FROM d
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, automatic_parallel_replicas_mode = 2,
    parallel_replicas_for_non_replicated_merge_tree = 1, prefer_localhost_replica = 0;

SELECT count(), sum(y) FROM d
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_only_with_analyzer = 1,
    enable_analyzer = 0, parallel_replicas_for_non_replicated_merge_tree = 1, prefer_localhost_replica = 0;

DROP TABLE d;
DROP TABLE t;
