-- Tags: shard

-- A `Merge` asks every child for a processing stage. A child column that does not convert to the
-- merged header type order-preservingly forces that stage down to `FetchColumns`, and a
-- `Distributed` child then forwards `FetchColumns` to the shard, which plans the whole query
-- there, PREWHERE included.

SET prefer_localhost_replica = 0;

CREATE TABLE t_fc_u (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_fc_i (k UInt64, v Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_fc_u SELECT number, number FROM numbers(4);
INSERT INTO t_fc_i SELECT number, number FROM numbers(4);
CREATE TABLE t_fc_dist_u AS t_fc_u ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_fc_u);
CREATE TABLE t_fc_dist_i AS t_fc_i ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_fc_i);

-- `v` is the column whose conversion into the merged header type is not order-preserving, so it is
-- what puts the queries below on the `FetchColumns` path. Pinned, so a change to the type
-- unification reddens here instead of leaving those queries exercising nothing.
DESCRIBE TABLE merge(currentDatabase(), '^t_fc_dist_[ui]$');

-- PREWHERE is evaluated by the reader itself, so its set has to be built on the shard. Every set
-- below is a strict subset of the keys present, so the counts also distinguish a read that applied
-- the predicate from one that ignored it.
SELECT count() FROM merge(currentDatabase(), '^t_fc_dist_[ui]$') PREWHERE k GLOBAL IN (SELECT k FROM t_fc_dist_u WHERE k < 2);
SELECT count() FROM merge(currentDatabase(), '^t_fc_dist_[ui]$') PREWHERE k GLOBAL IN (SELECT k FROM t_fc_dist_u WHERE k < 2) SETTINGS prefer_localhost_replica = 1;

-- Control: a single matched table needs no type unification, so its stage is delegated as usual and
-- this line passes without the fix. It keeps the test keyed on the stage rather than on `merge()`
-- plus `GLOBAL IN` in general.
SELECT count() FROM merge(currentDatabase(), '^t_fc_dist_u$') PREWHERE k GLOBAL IN (SELECT k FROM t_fc_dist_u WHERE k < 2);
