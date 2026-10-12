DROP TABLE IF EXISTS t;

CREATE TABLE t (
    key UInt32,
    value UInt32,
    PROJECTION p (SELECT sum(value) GROUP BY key)
) ENGINE = MergeTree ORDER BY key
SETTINGS materialize_projections_on_insert = 0, materialize_projections_on_merge = 1;

SYSTEM STOP MERGES t;

-- Insert two parts without projections
INSERT INTO t SELECT number, number FROM numbers(10);
INSERT INTO t SELECT number + 100, number FROM numbers(10);

-- No projection parts yet
SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't' AND active;

SYSTEM START MERGES t;

-- Merge rebuilds the projection
OPTIMIZE TABLE t FINAL;

-- Now one projection part exists on the merged part
SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't' AND active;

-- Projection is usable
SET optimize_use_projections = 1, force_optimize_projection = 1;
SELECT sum(value) FROM t WHERE key < 5;

DROP TABLE t;
