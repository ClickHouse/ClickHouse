-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS projection_expression_array_04839;

-- Same wrapper-mismatch class as 04839_json_shared_regexp_projection_expression_nullable.sql, but
-- for a projection expression that wraps its argument in a single-element array (array(j)) rather
-- than adding Nullable: the array's own element is the same JSON value j always was, so the policy
-- should still transfer even though the source column was never itself an Array.
CREATE TABLE projection_expression_array_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO projection_expression_array_04839 VALUES (1, '{"tag_a":1,"keep":1}');

-- Retire the rule at the table level; the existing part's own j type still carries it as history.
ALTER TABLE projection_expression_array_04839 MODIFY COLUMN j JSON(max_dynamic_paths=5);

ALTER TABLE projection_expression_array_04839
    ADD PROJECTION p (SELECT id, array(j) WHERE id > 0 ORDER BY id);
ALTER TABLE projection_expression_array_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=2;

-- Control: the projection's own column really is named after the expression and Array-wrapped.
SELECT
    'projection column name and type',
    countIf(column = 'array(j)' AND startsWith(type, 'Array'))
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='projection_expression_array_04839' AND part_name='p' AND active;

-- The regression: this must be 1. Without looking through the extra Array wrapper the
-- projection's own type has (that the bare source column j doesn't), the fallback finds j but
-- the merge itself is a no-op, and this comes back 0.
SELECT
    'projection expression array provenance',
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='projection_expression_array_04839' AND column='array(j)' AND active;

DROP TABLE projection_expression_array_04839;
