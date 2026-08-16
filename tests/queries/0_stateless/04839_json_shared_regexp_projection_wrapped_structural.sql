-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS wrapped_tuple_04839;

-- materialize()/CAST() don't change a tuple(...)/map(...)/arrayZip(...) expression's own structure;
-- per-slot provenance handling must still apply when they wrap it, not just at the top level.
CREATE TABLE wrapped_tuple_04839
(
    id UInt64,
    j1 JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_a'),
    j2 JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_b')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO wrapped_tuple_04839 VALUES (1, '{"tag_a1":1}', '{"tag_b1":2}');

ALTER TABLE wrapped_tuple_04839 MODIFY COLUMN j1 JSON(max_dynamic_paths=5);
ALTER TABLE wrapped_tuple_04839 MODIFY COLUMN j2 JSON(max_dynamic_paths=5);

ALTER TABLE wrapped_tuple_04839
    ADD PROJECTION p (SELECT id, materialize(tuple(j1, j2)) WHERE id > 0 ORDER BY id);
ALTER TABLE wrapped_tuple_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

-- The regression: both rules must survive on their own element through the materialize() wrapper.
SELECT
    'materialize(tuple(j1,j2)) retains both elements'' own provenance',
    countIf(position(type, '^tag_a') > 0 AND position(type, '^tag_b') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='wrapped_tuple_04839' AND column != 'id' AND active;

DROP TABLE wrapped_tuple_04839;
