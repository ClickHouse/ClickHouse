-- A JOIN sharded by primary-key ranges must keep working when the primary key prunes one of the sides
-- to nothing. The pruned side still has to produce one output port per layer, because the JOIN pairs
-- the ports of its two sides positionally.

DROP TABLE IF EXISTS t_05019_left;
DROP TABLE IF EXISTS t_05019_right;
DROP TABLE IF EXISTS t_05019_empty;

CREATE TABLE t_05019_left (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
CREATE TABLE t_05019_right (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
CREATE TABLE t_05019_empty (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;

INSERT INTO t_05019_left SELECT number, number * 2 FROM numbers(1000);
INSERT INTO t_05019_right SELECT number, number * 3 FROM numbers(1000);

-- The sharding applies only to reads in order and is not used for parallel replicas, so pin both
-- against the settings randomizer. `join_use_nulls` is pinned to keep the checksums below sensitive to
-- the unmatched rows.
SET query_plan_join_shard_by_pk_ranges = 1, join_algorithm = 'full_sorting_merge', max_threads = 4,
    optimize_read_in_order = 1, enable_parallel_replicas = 0, join_use_nulls = 0;

-- The right side is pruned to zero parts while the left side keeps its layers, and the other way around.
SELECT count() FROM t_05019_left AS l JOIN (SELECT * FROM t_05019_right WHERE k > 1000000) AS r ON l.k = r.k;
SELECT count() FROM (SELECT * FROM t_05019_left WHERE k > 1000000) AS l JOIN t_05019_right AS r ON l.k = r.k;

-- An empty table is pruned the same way. This is how the failure showed up in CI.
SELECT count() FROM t_05019_left AS l JOIN t_05019_empty AS r ON l.k = r.k;
SELECT count() FROM t_05019_empty AS l JOIN t_05019_right AS r ON l.k = r.k;

-- With a pruned side in a LEFT or a RIGHT join, every row of the other side reaches the output, so a
-- shard paired with the wrong one changes the result and not only the number of rows. Compare against
-- `hash`, which does not use this pipeline.
SELECT
    (SELECT (count(), sum(cityHash64(l.k, l.v, r.k, r.v))) FROM t_05019_left AS l LEFT JOIN (SELECT * FROM t_05019_right WHERE k > 1000000) AS r ON l.k = r.k)
  = (SELECT (count(), sum(cityHash64(l.k, l.v, r.k, r.v))) FROM t_05019_left AS l LEFT JOIN (SELECT * FROM t_05019_right WHERE k > 1000000) AS r ON l.k = r.k SETTINGS join_algorithm = 'hash');

SELECT
    (SELECT (count(), sum(cityHash64(l.k, l.v, r.k, r.v))) FROM (SELECT * FROM t_05019_left WHERE k > 1000000) AS l RIGHT JOIN t_05019_right AS r ON l.k = r.k)
  = (SELECT (count(), sum(cityHash64(l.k, l.v, r.k, r.v))) FROM (SELECT * FROM t_05019_left WHERE k > 1000000) AS l RIGHT JOIN t_05019_right AS r ON l.k = r.k SETTINGS join_algorithm = 'hash');

-- Nothing is pruned: the sharded pipeline itself still has to produce the same result as `hash`.
SELECT
    (SELECT (count(), sum(cityHash64(l.k, l.v, r.k, r.v))) FROM t_05019_left AS l JOIN t_05019_right AS r ON l.k = r.k)
  = (SELECT (count(), sum(cityHash64(l.k, l.v, r.k, r.v))) FROM t_05019_left AS l JOIN t_05019_right AS r ON l.k = r.k SETTINGS join_algorithm = 'hash');

DROP TABLE t_05019_left;
DROP TABLE t_05019_right;
DROP TABLE t_05019_empty;
