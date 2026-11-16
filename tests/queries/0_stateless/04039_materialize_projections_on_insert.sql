DROP TABLE IF EXISTS t;

CREATE TABLE t (
    key UInt32,
    value UInt32,
    PROJECTION p (SELECT sum(value) GROUP BY key)
) ENGINE = MergeTree ORDER BY key;

SYSTEM STOP MERGES t;

-- Default (= 1): projection is built during insert
INSERT INTO t SELECT number, number FROM numbers(10);
SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't' AND active;

-- Disabled (= 0): projection is NOT built during insert
ALTER TABLE t MODIFY SETTING materialize_projections_on_insert = 0;
INSERT INTO t SELECT number + 100, number FROM numbers(10);
-- Still 1: the second part has no projection
SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't' AND active;

SYSTEM START MERGES t;
DROP TABLE t;
