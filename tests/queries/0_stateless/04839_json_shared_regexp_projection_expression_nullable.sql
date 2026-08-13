-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS projection_expression_nullable_04839;

-- Mirror image of 04839_json_shared_regexp_projection_expression.sql: there, assumeNotNull(j)
-- strips a Nullable the source still has; here, toNullable(j) adds one the source doesn't have.
-- mergeJSONSharedDataPathRules requires matching type shapes and otherwise silently no-ops, so
-- this direction needs the same wrapper-mismatch handling, just the other way around.
CREATE TABLE projection_expression_nullable_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO projection_expression_nullable_04839 VALUES (1, '{"tag_a":1,"keep":1}');

-- Retire the rule at the table level; the existing part's own j type still carries it as history.
ALTER TABLE projection_expression_nullable_04839 MODIFY COLUMN j JSON(max_dynamic_paths=5);

ALTER TABLE projection_expression_nullable_04839
    ADD PROJECTION p (SELECT id, toNullable(j) WHERE id > 0 ORDER BY id);
ALTER TABLE projection_expression_nullable_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=2;

-- Control: the projection's own column really is named after the expression and Nullable-wrapped.
SELECT
    'projection column name and type',
    countIf(column = 'toNullable(j)' AND startsWith(type, 'Nullable'))
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='projection_expression_nullable_04839' AND part_name='p' AND active;

-- The regression: this must be 1. Without looking through the extra Nullable the projection's
-- own type has (that the bare source column j doesn't), the fallback finds j but the merge
-- itself is a no-op, and this comes back 0.
SELECT
    'projection expression nullable provenance',
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='projection_expression_nullable_04839' AND column='toNullable(j)' AND active;

DROP TABLE projection_expression_nullable_04839;
