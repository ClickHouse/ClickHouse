-- With materialize_projections_on_merge, a projection that is missing from the source parts
-- is rebuilt when those parts are merged. Parts with *different* projection sets are still never
-- merged together: this is a deliberate trade-off so that a small part missing a projection is not
-- merged into a much larger part that already has it (which would rebuild the projection for all rows).
-- Missing projections are instead filled in gradually as parts of the same projection set are merged.

DROP TABLE IF EXISTS t;

CREATE TABLE t
(
    key UInt32,
    value UInt64,
    PROJECTION p1 (SELECT key, sum(value) GROUP BY key)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS materialize_projections_on_merge = 1;

-- These parts have projection p1.
INSERT INTO t SELECT number, number FROM numbers(3);
INSERT INTO t SELECT number + 10, number FROM numbers(3);

-- Add a second projection. Existing parts do not have it yet, so all active parts share the set {p1}.
ALTER TABLE t ADD PROJECTION p2 (SELECT key, max(value) GROUP BY key);

-- Parts have the same projection set {p1}, so they can be merged, and p2 is rebuilt during the merge.
OPTIMIZE TABLE t FINAL SETTINGS optimize_throw_if_noop = 1;

-- A single part remains, with both projections.
SELECT count() AS num_parts, arraySort(groupUniqArrayArray(projections)) AS all_projections
FROM system.parts
WHERE database = currentDatabase() AND table = 't' AND active;

DROP TABLE t;

-- Now create parts with different projection sets and check they are not merged together.
CREATE TABLE t
(
    key UInt32,
    value UInt64,
    PROJECTION p1 (SELECT key, sum(value) GROUP BY key)
)
ENGINE = MergeTree
ORDER BY key
SETTINGS materialize_projections_on_merge = 1;

-- These parts have only p1.
INSERT INTO t SELECT number, number FROM numbers(3);
INSERT INTO t SELECT number + 10, number FROM numbers(3);

ALTER TABLE t ADD PROJECTION p2 (SELECT key, max(value) GROUP BY key);

-- These parts have both p1 and p2.
INSERT INTO t SELECT number + 20, number FROM numbers(3);
INSERT INTO t SELECT number + 30, number FROM numbers(3);

-- Parts now have different projection sets ({p1} and {p1, p2}) and cannot be merged together,
-- even with materialize_projections_on_merge = 1.
OPTIMIZE TABLE t FINAL SETTINGS optimize_throw_if_noop = 1; -- { serverError CANNOT_ASSIGN_OPTIMIZE }

DROP TABLE t;
