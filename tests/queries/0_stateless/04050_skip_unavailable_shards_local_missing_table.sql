-- Tags: shard, no-fasttest

-- Test that skip_unavailable_shards skips local shards with missing tables
-- but still throws when all shards end up being skipped.
-- https://github.com/ClickHouse/ClickHouse/issues/100134

DROP TABLE IF EXISTS dist_04050;
DROP TABLE IF EXISTS dist_04050_two_shards;

CREATE TABLE dist_04050 (x UInt32)
ENGINE = Distributed(test_shard_localhost, currentDatabase(), non_existent_table_04050);

-- Without skip_unavailable_shards, the query should fail with UNKNOWN_TABLE
SELECT * FROM dist_04050 SETTINGS prefer_localhost_replica = 1; -- { serverError UNKNOWN_TABLE }

-- With skip_unavailable_shards, the shard is skipped, but since it is the only shard,
-- there are zero available shards and the query should still fail.
SELECT * FROM dist_04050 SETTINGS skip_unavailable_shards = 1, prefer_localhost_replica = 1; -- { serverError ALL_CONNECTION_TRIES_FAILED }

-- Two-shard cluster where the local table is missing on both shards; the skip path must honour
-- max_skip_unavailable_shards_num / max_skip_unavailable_shards_ratio.
-- Use the cluster whose shards both resolve to `localhost` so each shard is detected as local
-- (`127.0.0.{2..255}` is intentionally not treated as a local address by ClickHouse, see
-- `isLocalAddress.cpp`, which would route the second shard through the remote path instead).
CREATE TABLE dist_04050_two_shards (x UInt32)
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), non_existent_table_04050);

-- max_skip_unavailable_shards_num = 1: skipping more than one shard must throw.
SELECT * FROM dist_04050_two_shards
SETTINGS skip_unavailable_shards = 1, prefer_localhost_replica = 1, max_skip_unavailable_shards_num = 1; -- { serverError TOO_MANY_UNAVAILABLE_SHARDS }

-- max_skip_unavailable_shards_ratio = 0.4: skipping 100% (both of two) must throw.
SELECT * FROM dist_04050_two_shards
SETTINGS skip_unavailable_shards = 1, prefer_localhost_replica = 1, max_skip_unavailable_shards_ratio = 0.4; -- { serverError TOO_MANY_UNAVAILABLE_SHARDS }

DROP TABLE dist_04050;
DROP TABLE dist_04050_two_shards;
