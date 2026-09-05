-- Tags: no-parallel, shard

-- A shard that is itself a `Distributed` with no shard left raises `ALL_CONNECTION_TRIES_FAILED`
-- from a host that answered, so it reaches the outer query as an exception rather than as an
-- unavailable shard. Whether the outer query keeps the rows its other shard produced is decided by
-- `skip_unavailable_shards_mode`, and only the mode that ignores an exception raised before any
-- data keeps them.
-- The databases the cluster names below are global, hence `no-parallel`; the parallel-safe arms of
-- this behaviour live in 05031.
-- https://github.com/ClickHouse/ClickHouse/issues/115646

DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;
CREATE DATABASE shard_0;
CREATE DATABASE shard_1;

CREATE TABLE shard_0.data_05032 (c0 UInt64) ENGINE = MergeTree() PRIMARY KEY tuple();
INSERT INTO shard_0.data_05032 SELECT number FROM numbers(3);

-- Shard 1 of the cluster resolves in `shard_0` and reads the table above. Shard 2 resolves in
-- `shard_1` and reads a `Distributed` whose own nodes are all at a dead port.
CREATE TABLE shard_0.leaf_05032 (c0 UInt64) ENGINE = MergeTree() PRIMARY KEY tuple();
INSERT INTO shard_0.leaf_05032 SELECT number FROM numbers(3);
CREATE TABLE shard_1.leaf_05032 (c0 UInt64)
ENGINE = Distributed(test_cluster_multiple_nodes_all_unavailable, shard_0, data_05032);

CREATE TABLE shard_0.outer_05032 (c0 UInt64)
ENGINE = Distributed(test_cluster_two_shards_different_databases, '', leaf_05032);

-- Both shards reading the healthy leaf, so a later empty result is the nested layer and not the
-- fixture failing to answer.
CREATE TABLE shard_0.outer_ok_05032 (c0 UInt64)
ENGINE = Distributed(test_cluster_two_shards, shard_0, leaf_05032);

SELECT count() FROM shard_0.outer_ok_05032 SETTINGS prefer_localhost_replica = 0;

-- The two strict modes report the inner error, dropping what the healthy shard produced.
SELECT count() FROM shard_0.outer_05032
SETTINGS prefer_localhost_replica = 0, skip_unavailable_shards = 1,
         skip_unavailable_shards_mode = 'unavailable_or_table_missing'; -- { serverError ALL_CONNECTION_TRIES_FAILED }
SELECT count() FROM shard_0.outer_05032
SETTINGS prefer_localhost_replica = 0, skip_unavailable_shards = 1,
         skip_unavailable_shards_mode = 'unavailable'; -- { serverError ALL_CONNECTION_TRIES_FAILED }

-- The tolerant mode skips the nested shard and keeps them.
SELECT count() FROM shard_0.outer_05032
SETTINGS prefer_localhost_replica = 0, skip_unavailable_shards = 1,
         skip_unavailable_shards_mode = 'unavailable_or_exception_before_processing';

-- One outer shard skipped, so the outer query did not adopt the inner error as its own. The strict
-- modes above skip none and report it instead, which the row count alone does not distinguish.
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['DistributedShardsSkipped']
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish'
  AND query LIKE '%FROM shard\_0.outer\_05032%'
  AND query LIKE '%unavailable\_or\_exception\_before\_processing%'
  AND query NOT LIKE '%system.query\_log%';

DROP DATABASE shard_0;
DROP DATABASE shard_1;
