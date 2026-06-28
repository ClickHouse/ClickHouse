-- Tags: no-parallel, no-flaky-check, shard

-- Tests the `skip_unavailable_shards_mode` setting on the SELECT path when a shard's table is missing.
-- A missing table is detected before any query is sent (the replica is dropped from `getTablesStatus`),
-- so the shard reaches the initiator as zero usable connections rather than as a server exception.
-- The `unavailable` mode must still honor the missing-table part of the contract and fail, instead of
-- silently skipping the shard. The other two modes skip it.
-- The underlying table exists only on shard_0; it is missing on shard_1. `prefer_localhost_replica = 0`
-- forces the connection (remote) path so the missing-table detection happens during connection establishment.

DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;
CREATE DATABASE shard_0;
CREATE DATABASE shard_1;

CREATE TABLE shard_0.t_sus (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO shard_0.t_sus VALUES (10), (20);

CREATE TABLE dist_sus (x UInt32) ENGINE = Distributed('test_cluster_two_shards_different_databases', '', t_sus, x);

-- `unavailable_or_table_missing`: shard_1 (missing table) is skipped; only shard_0's rows are returned.
SELECT x FROM dist_sus ORDER BY x
SETTINGS prefer_localhost_replica = 0, skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_table_missing';

-- `unavailable_or_exception_before_processing` also skips the shard whose table is missing.
SELECT x FROM dist_sus ORDER BY x
SETTINGS prefer_localhost_replica = 0, skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_exception_before_processing';

-- `unavailable`: a missing table is not a connection error, so the shard must not be silently skipped.
SELECT x FROM dist_sus ORDER BY x
SETTINGS prefer_localhost_replica = 0, skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable'; -- { serverError UNKNOWN_TABLE }

DROP TABLE dist_sus;
DROP DATABASE shard_0;
DROP DATABASE shard_1;
