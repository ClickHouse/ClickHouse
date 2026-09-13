-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS projection_json_path_04839;

-- A projection reading a JSON path resolves through a path step, not through a physical column, so
-- the provenance descent has to follow it: the nested object at `arr` is stored under the type
-- derived for that path, and that is where the retired rule survives on the source part.
CREATE TABLE projection_json_path_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_')
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO projection_json_path_04839 VALUES (1, '{"arr":{"tag_a":1,"keep":2}}');

-- Retire the rule at the table level; the part's own j type still carries it as history.
ALTER TABLE projection_json_path_04839 MODIFY COLUMN j JSON(max_dynamic_paths=5);

ALTER TABLE projection_json_path_04839
    ADD PROJECTION p_sub_object (SELECT id, j.^arr WHERE id > 0 ORDER BY id);
ALTER TABLE projection_json_path_04839 MATERIALIZE PROJECTION p_sub_object SETTINGS mutations_sync=1;

-- The regression: without the JSON path step in the descent this is 0, and the next rewrite of the
-- projection is free to re-promote the nested paths the retired rule had forced into shared data.
SELECT 'sub-object path keeps provenance', countIf(position(type, '^tag_') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='projection_json_path_04839' AND name='p_sub_object' AND column != 'id' AND active;

-- The literal path and the combined accessor read a `Dynamic`, which has no JSON node to hold a
-- policy. Nothing is lost there: the nested object travels as a Dynamic variant that still spells
-- the rule in its own type.
SELECT 'path accessors are Dynamic', toTypeName(j.arr), toTypeName(j.@arr) FROM projection_json_path_04839;

DROP TABLE projection_json_path_04839;
