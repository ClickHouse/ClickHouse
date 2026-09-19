-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS projection_rebuild_04839;

-- A merge rebuilding a projection sub-part that predates a later ADD COLUMN must recover that
-- column's provenance by falling back to the source part's own physical column, per column.
CREATE TABLE projection_rebuild_04839
(
    id UInt64,
    x UInt64,
    PROJECTION p (SELECT * ORDER BY id)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part=0,
    min_rows_for_wide_part=0,
    max_bytes_to_merge_at_max_space_in_pool=1;

INSERT INTO projection_rebuild_04839 VALUES (1, 100);

ALTER TABLE projection_rebuild_04839
    ADD COLUMN j JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_') DEFAULT '{}';

-- Force the rule to become the first part's own physical provenance for j, without touching
-- its (still stale, j-less) projection sub-part.
ALTER TABLE projection_rebuild_04839 MATERIALIZE COLUMN j SETTINGS mutations_sync=1;

-- Retire the rule at the table level; the first part's own j type still carries it as history.
ALTER TABLE projection_rebuild_04839 MODIFY COLUMN j JSON(max_dynamic_paths=5);

-- SYSTEM STOP MERGES only from here: MATERIALIZE COLUMN above is itself a mutation, and stopping
-- merges also blocks mutations from executing.
SYSTEM STOP MERGES projection_rebuild_04839;

INSERT INTO projection_rebuild_04839 VALUES (2, 200, '{"tag_b":2,"keep":2}');

SELECT
    'before merge',
    count(),
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.parts_columns
WHERE database=currentDatabase() AND table='projection_rebuild_04839' AND column='j' AND active;

SELECT
    'before merge projection coverage',
    countDistinct(part_name)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='projection_rebuild_04839' AND column='j' AND active;

SYSTEM START MERGES projection_rebuild_04839;
OPTIMIZE TABLE projection_rebuild_04839 FINAL;

SELECT
    'after merge',
    count(),
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.parts_columns
WHERE database=currentDatabase() AND table='projection_rebuild_04839' AND column='j' AND active;

-- The regression: this must be 1. Buggy code silently drops provenance for a source part whose
-- projection sub-part exists but lacks the column, producing 0 here instead.
SELECT
    'after merge projection provenance',
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='projection_rebuild_04839' AND column='j' AND active;

DROP TABLE projection_rebuild_04839;
