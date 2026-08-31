-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/116908
-- `optimize_skip_unused_shards_rewrite_in` prunes the elements of an `IN` set over the sharding
-- column to the ones routed to each shard. For an `IN` in the `SELECT` projection that changes the
-- result column's name, so the initiator could not bind the column the shard returned and the query
-- threw instead of returning rows. The rewrite is now confined to everything but the projection.
--
-- Both shards of `test_cluster_two_shards` read the same `system.one`, which does not respect the
-- sharding key, so the pruning legitimately changes how many rows come back; the point here is that
-- the queries return their rows at all.

SET prefer_localhost_replica = 0;
SET optimize_skip_unused_shards = 1;
SET optimize_skip_unused_shards_rewrite_in = 1;

DROP TABLE IF EXISTS dist_05071;
CREATE TABLE dist_05071 AS system.one ENGINE = Distributed(test_cluster_two_shards, system, one, intHash64(dummy));

-- intHash64(0) % 2 = 0 and intHash64(2) % 2 = 1, so both shards survive the pruning.
SELECT dummy, dummy IN (0, 2) FROM dist_05071 WHERE dummy IN (0, 2) ORDER BY dummy;
SELECT dummy, (dummy IN (0, 2)) AS f FROM dist_05071 WHERE dummy IN (0, 2) ORDER BY dummy, f;
SELECT dummy IN (0, 2) FROM dist_05071 ORDER BY dummy;
SELECT dummy, dummy IN (0, 2), dummy IN (1, 3) FROM dist_05071 WHERE dummy IN (0, 2) ORDER BY dummy;

-- A `WHERE`-only shape keeps being rewritten: shard 1 gets `IN (0)` and shard 2 gets `IN (2)`, so
-- only one of them has a matching row, while without the rewrite both do.
SELECT count() FROM dist_05071 WHERE dummy IN (0, 2);
SELECT count() FROM dist_05071 WHERE dummy IN (0, 2) SETTINGS optimize_skip_unused_shards_rewrite_in = 0;

DROP TABLE dist_05071;
