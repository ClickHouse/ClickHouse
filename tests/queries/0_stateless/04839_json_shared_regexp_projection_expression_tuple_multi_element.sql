-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS tuple_ambiguity_04839;

-- Unlike the single-element wrapper case covered elsewhere, a multi-element tuple is ambiguous in
-- general -- except when exactly one element is JSON-shaped, since only that one can own the policy.
CREATE TABLE tuple_ambiguity_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_'),
    t Tuple(UInt8, JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_'))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO tuple_ambiguity_04839 VALUES (1, '{"tag_a":1}', (1, '{"tag_a":1}'));

-- Retire the rule from the current metadata first, like the sibling projection tests: otherwise the
-- projection column type carries it straight from the projection metadata and both assertions below
-- would hold even with the provenance reconstruction broken.
ALTER TABLE tuple_ambiguity_04839
    MODIFY COLUMN j JSON(max_dynamic_paths=5),
    MODIFY COLUMN t Tuple(UInt8, JSON(max_dynamic_paths=5));

ALTER TABLE tuple_ambiguity_04839 ADD PROJECTION p1 (SELECT id, tuple(j, 1) WHERE id > 0 ORDER BY id);
ALTER TABLE tuple_ambiguity_04839 ADD PROJECTION p2 (SELECT id, tupleElement(t, 2) WHERE id > 0 ORDER BY id);
ALTER TABLE tuple_ambiguity_04839 MATERIALIZE PROJECTION p1 SETTINGS mutations_sync=1;
ALTER TABLE tuple_ambiguity_04839 MATERIALIZE PROJECTION p2 SETTINGS mutations_sync=1;

-- p1: tuple(j, 1) -- the sole JSON-shaped element (position 0) must keep j's SHARED REGEXP rule.
SELECT
    'tuple(j, 1) retains provenance on the JSON element',
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='tuple_ambiguity_04839' AND part_name='p1' AND column != 'id' AND active;

-- p2: tupleElement(t, 2) -- extracting the sole JSON-shaped element of a multi-element source
-- tuple must still resolve back to t and keep the rule.
SELECT
    'tupleElement(t, 2) retains provenance from the JSON element',
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='tuple_ambiguity_04839' AND part_name='p2' AND column != 'id' AND active;

DROP TABLE tuple_ambiguity_04839;
