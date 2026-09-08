-- Tags: distributed

-- A `LIMIT [n] AFTER/UNTIL` range in a view body is applied once on the initiator, so the body is not
-- a trivial view and `optimize_trivial_view_pushdown_to_distributed` must not forward it to the shards,
-- where every shard would apply the range to its own rows. Both shards of the cluster read the same
-- local table, so the two shards return the same five rows each.
SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS t_trivial_range_local;
DROP TABLE IF EXISTS t_trivial_range_dist;
DROP VIEW IF EXISTS v_trivial_range_window;
DROP VIEW IF EXISTS v_trivial_range_until;
DROP VIEW IF EXISTS v_trivial_range_counted;

CREATE TABLE t_trivial_range_local (x UInt64) ENGINE = Memory;
CREATE TABLE t_trivial_range_dist AS t_trivial_range_local
    ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_trivial_range_local);
INSERT INTO t_trivial_range_local SELECT number FROM numbers(5);

CREATE VIEW v_trivial_range_window AS SELECT x FROM t_trivial_range_dist LIMIT AFTER x = 3 UNTIL x = 4;
CREATE VIEW v_trivial_range_until AS SELECT x FROM t_trivial_range_dist LIMIT UNTIL x = 2;
CREATE VIEW v_trivial_range_counted AS SELECT x FROM t_trivial_range_dist LIMIT 1 AFTER x >= 0;

-- Each shard delivers its five rows as a single block, so on the initiator the range closes inside
-- the first block it receives, whichever shard sent it: one row `3`, not one per shard.
SELECT count(), groupArray(x) FROM v_trivial_range_window SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;
SELECT count(), groupArray(x) FROM v_trivial_range_window SETTINGS optimize_trivial_view_pushdown_to_distributed = 1;

-- Two rows in total, not two per shard.
SELECT count() FROM v_trivial_range_until SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;
SELECT count() FROM v_trivial_range_until SETTINGS optimize_trivial_view_pushdown_to_distributed = 1;

-- A counted range already keeps the view non-trivial through its `LIMIT`.
SELECT count(), groupArray(x) FROM v_trivial_range_counted SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;
SELECT count(), groupArray(x) FROM v_trivial_range_counted SETTINGS optimize_trivial_view_pushdown_to_distributed = 1;

DROP VIEW v_trivial_range_window;
DROP VIEW v_trivial_range_until;
DROP VIEW v_trivial_range_counted;
DROP TABLE t_trivial_range_dist;
DROP TABLE t_trivial_range_local;
