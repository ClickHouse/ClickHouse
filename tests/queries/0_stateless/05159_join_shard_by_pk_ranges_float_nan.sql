-- The layer split of the PK-range join sharding compares key values as `greater(tuple(pk), tuple(border))`,
-- and an IEEE comparison is false for `NaN` against anything, so a row with a `NaN` key must not be
-- routed by it: the join has to return the same rows with and without the optimization.

DROP TABLE IF EXISTS t_join_shard_left;
DROP TABLE IF EXISTS t_join_shard_right;

CREATE TABLE t_join_shard_left (k Float64, v UInt32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
CREATE TABLE t_join_shard_right (k Float64, v UInt32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;

INSERT INTO t_join_shard_left SELECT number, number FROM numbers(500);
INSERT INTO t_join_shard_left SELECT 250.5 + number, 10000 + number FROM numbers(500);
INSERT INTO t_join_shard_left SELECT nan, 90000 + number FROM numbers(5);

INSERT INTO t_join_shard_right SELECT number, number FROM numbers(500);
INSERT INTO t_join_shard_right SELECT 250.5 + number, 20000 + number FROM numbers(500);
INSERT INTO t_join_shard_right SELECT nan, 95000 + number FROM numbers(5);

SET join_algorithm = 'full_sorting_merge';

SELECT count() FROM t_join_shard_left INNER JOIN t_join_shard_right ON t_join_shard_left.k = t_join_shard_right.k SETTINGS query_plan_join_shard_by_pk_ranges = 0;
SELECT count() FROM t_join_shard_left INNER JOIN t_join_shard_right ON t_join_shard_left.k = t_join_shard_right.k SETTINGS query_plan_join_shard_by_pk_ranges = 1;

SELECT count() FROM t_join_shard_left LEFT JOIN t_join_shard_right ON t_join_shard_left.k = t_join_shard_right.k SETTINGS query_plan_join_shard_by_pk_ranges = 0;
SELECT count() FROM t_join_shard_left LEFT JOIN t_join_shard_right ON t_join_shard_left.k = t_join_shard_right.k SETTINGS query_plan_join_shard_by_pk_ranges = 1;

DROP TABLE t_join_shard_left;
DROP TABLE t_join_shard_right;

-- An integer key still shards: the same counts with the optimization on and off.

CREATE TABLE t_join_shard_left (k UInt64, v UInt32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
CREATE TABLE t_join_shard_right (k UInt64, v UInt32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;

INSERT INTO t_join_shard_left SELECT number % 300, number FROM numbers(1000);
INSERT INTO t_join_shard_right SELECT number % 300, number FROM numbers(1000);

SELECT count() FROM t_join_shard_left INNER JOIN t_join_shard_right ON t_join_shard_left.k = t_join_shard_right.k SETTINGS query_plan_join_shard_by_pk_ranges = 0;
SELECT count() FROM t_join_shard_left INNER JOIN t_join_shard_right ON t_join_shard_left.k = t_join_shard_right.k SETTINGS query_plan_join_shard_by_pk_ranges = 1;

DROP TABLE t_join_shard_left;
DROP TABLE t_join_shard_right;
