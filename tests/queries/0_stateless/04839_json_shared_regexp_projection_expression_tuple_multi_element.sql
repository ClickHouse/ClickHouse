-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS tuple_ambiguity_04839;

-- 04839_json_shared_regexp_projection_expression_tuple(_element).sql cover the single-element
-- wrapper case (tuple(j) / tupleElement(t, 1) over a single-element t). A tuple with more than one
-- element is ambiguous in general, but not when exactly one element is JSON-shaped: there is then
-- only one element the policy could unambiguously belong to.
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
