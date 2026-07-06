-- Regression test for the `WHERE <field>` index-skipping feature (#89222) interacting
-- with aggregate projections.
--
-- Since #89222 a bare numeric key column is read as `key != 0` for primary-key and
-- skip-index analysis. When reading from an aggregate projection without a `WHERE`
-- clause, the projection's first output is a key column (not a filter predicate), so it
-- must NOT be misinterpreted as such a condition. Otherwise a projection part whose only
-- value of the key is 0 is incorrectly pruned. The zero values are inserted in their own
-- parts so that the buggy pruning has a whole granule (min = max = 0) to drop.

DROP TABLE IF EXISTS t_agg_proj;

CREATE TABLE t_agg_proj (x UInt8, PROJECTION p (SELECT x GROUP BY x))
ENGINE = MergeTree ORDER BY ();

INSERT INTO t_agg_proj VALUES (0);
INSERT INTO t_agg_proj VALUES (1), (2);

SELECT '-- no WHERE, aggregate projection used: the x = 0 group must be present';
SELECT x FROM t_agg_proj GROUP BY x ORDER BY x
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_agg_proj;

-- Same, but with an aggregate function and a non-empty primary key on the main table.
DROP TABLE IF EXISTS t_agg_proj2;

CREATE TABLE t_agg_proj2 (x Int32, y UInt32, PROJECTION p (SELECT x, sum(y) GROUP BY x))
ENGINE = MergeTree ORDER BY y;

INSERT INTO t_agg_proj2 VALUES (0, 10), (0, 20);
INSERT INTO t_agg_proj2 VALUES (1, 30), (-5, 40);

SELECT '-- aggregate projection with sum, the x = 0 group must be present';
SELECT x, sum(y) FROM t_agg_proj2 GROUP BY x ORDER BY x
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_agg_proj2;

-- The `WHERE <field>` feature itself still applies when reading through a projection:
-- `WHERE flag` keeps only the rows where flag is non-zero, so the flag = 0 group is excluded.
DROP TABLE IF EXISTS t_where_field;

CREATE TABLE t_where_field (flag UInt8, y UInt32, PROJECTION p (SELECT flag, sum(y) GROUP BY flag))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_where_field VALUES (0, 10);
INSERT INTO t_where_field VALUES (1, 30), (1, 40);

SELECT '-- WHERE flag (bare column) over a projection: the flag = 0 group is excluded';
SELECT flag, sum(y) FROM t_where_field WHERE flag GROUP BY flag ORDER BY flag
SETTINGS optimize_use_projections = 1;

DROP TABLE t_where_field;
