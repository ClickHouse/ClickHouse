-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS projection_rename_04839;

-- A projection rebuild (merge-time, or mutation-time MATERIALIZE PROJECTION) resolves each column's
-- provenance by candidate name against the source part's own columns (see
-- applyJSONSharedDataPathPoliciesForProjection, MergeTreeDataWriter.cpp). After RENAME COLUMN, a
-- source part written before the rename still has the OLD physical name; without resolving through
-- that part's own AlterConversions rename map first -- the same way the main (non-projection)
-- provenance merge and IMergeTreeReader::getStorageAndSubcolumnNameInPart already do -- the fallback
-- misses the old physical column entirely and silently drops the historical SHARED REGEXP policy.
--
-- RENAME COLUMN refuses to apply if an *existing* projection still references the old name, so the
-- projection here is added and materialized after the rename -- referencing the new name only, the
-- same way a real ADD PROJECTION would after a prior rename -- while the source part being
-- materialized against still physically has the pre-rename column.
CREATE TABLE projection_rename_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO projection_rename_04839 VALUES (1, '{"tag_a":1,"keep":1}');

-- Retire the rule at the table level; the existing part's own j type still carries it as history.
ALTER TABLE projection_rename_04839 MODIFY COLUMN j JSON(max_dynamic_paths=5);

-- Rename the column. The existing part still physically has "j"; only the logical/metadata name is
-- now "payload".
ALTER TABLE projection_rename_04839 RENAME COLUMN j TO payload;

ALTER TABLE projection_rename_04839
    ADD PROJECTION p (SELECT id, payload WHERE id > 0 ORDER BY id);
ALTER TABLE projection_rename_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=2;

-- Control: the projection's own column really is named after the renamed column.
SELECT
    'projection column name',
    countIf(column = 'payload')
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='projection_rename_04839' AND part_name='p' AND active;

-- The regression: this must be 1. Without resolving "payload" back through the source part's own
-- rename map to "j" before probing it, the rebuilt projection's column falls back to the current
-- bare type and this comes back 0.
SELECT
    'projection provenance after rename',
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='projection_rename_04839' AND column='payload' AND active;

DROP TABLE projection_rename_04839;
