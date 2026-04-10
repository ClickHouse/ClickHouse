-- Test that materialize_projections_on_merge allows merging parts with different projection sets.
-- Without this setting, parts with different projection sets cannot be merged together.

DROP TABLE IF EXISTS t;

-- Create table with one projection
CREATE TABLE t
(
    `key` UInt32,
    `value` UInt64,
    PROJECTION p1
    (
        SELECT key, sum(value)
        GROUP BY key
    )
)
ENGINE = MergeTree
ORDER BY key
SETTINGS materialize_projections_on_merge = 1;

-- Insert data (these parts will have projection p1)
INSERT INTO t SELECT number, number FROM numbers(3);
INSERT INTO t SELECT number + 10, number FROM numbers(3);

-- Add a second projection
ALTER TABLE t ADD PROJECTION p2 (SELECT key, max(value) GROUP BY key);

-- Insert more data (these parts will have both p1 and p2)
INSERT INTO t SELECT number + 20, number FROM numbers(3);
INSERT INTO t SELECT number + 30, number FROM numbers(3);

-- Now we have parts with different projection sets: {p1} vs {p1, p2}.
-- With materialize_projections_on_merge, OPTIMIZE should succeed and rebuild p2 for the older parts.
OPTIMIZE TABLE t FINAL;

-- Verify data is correct
SELECT key, sum(value) FROM t GROUP BY key ORDER BY key
SETTINGS optimize_use_projections = 1;

-- Verify that after merge we have a single active part with both projections
SELECT
    count() AS num_parts,
    arraySort(groupUniqArrayArray(projections)) AS all_projections
FROM system.parts
WHERE database = currentDatabase() AND table = 't' AND active;

DROP TABLE t;

-- Test that without the setting, OPTIMIZE FINAL does not merge parts with different projection sets
CREATE TABLE t
(
    `key` UInt32,
    `value` UInt64,
    PROJECTION p1
    (
        SELECT key, sum(value)
        GROUP BY key
    )
)
ENGINE = MergeTree
ORDER BY key
SETTINGS materialize_projections_on_merge = 0;

INSERT INTO t SELECT number, number FROM numbers(3);
INSERT INTO t SELECT number + 10, number FROM numbers(3);

ALTER TABLE t ADD PROJECTION p2 (SELECT key, max(value) GROUP BY key);

INSERT INTO t SELECT number + 20, number FROM numbers(3);
INSERT INTO t SELECT number + 30, number FROM numbers(3);

OPTIMIZE TABLE t FINAL SETTINGS optimize_throw_if_noop = 1; -- { serverError CANNOT_ASSIGN_OPTIMIZE }

DROP TABLE t;
