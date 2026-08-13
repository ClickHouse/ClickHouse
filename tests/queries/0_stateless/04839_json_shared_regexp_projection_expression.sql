-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS projection_expression_04839;

-- ProjectionsDescription.cpp picks QueryProcessingStage based on whether the projection has a
-- WHERE clause (or aggregation): without one, the header -- and so a materialized part with no
-- prior projection sub-part to read from -- is built straight from the source columns verbatim,
-- so a non-trivial SELECT-list expression's *own* name never actually reaches the projection's
-- metadata. With WHERE present, the header comes from real analyzed expression-result columns
-- instead, and the projection's physical column is genuinely named after the expression (e.g.
-- `assumeNotNull(j)`), not the source column it reads from -- so the provenance fallback has to
-- resolve back through the expression's referenced identifiers, not just the output name.
CREATE TABLE projection_expression_04839
(
    id UInt64,
    j Nullable(JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_'))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO projection_expression_04839 VALUES (1, '{"tag_a":1,"keep":1}');

-- Retire the rule at the table level; the existing part's own j type still carries it as history.
ALTER TABLE projection_expression_04839 MODIFY COLUMN j Nullable(JSON(max_dynamic_paths=5));

ALTER TABLE projection_expression_04839
    ADD PROJECTION p (SELECT id, assumeNotNull(j) WHERE id > 0 ORDER BY id);
ALTER TABLE projection_expression_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=2;

-- Control: the projection's own column really is named after the expression, not the source.
SELECT
    'projection column name',
    countIf(column = 'assumeNotNull(j)'),
    countIf(column = 'j')
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='projection_expression_04839' AND part_name='p' AND active;

-- The regression: this must be 1. Without resolving `assumeNotNull(j)` back to its referenced
-- identifier `j`, the fallback misses the parent part's column entirely and this comes back 0.
SELECT
    'projection expression provenance',
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='projection_expression_04839' AND column='assumeNotNull(j)' AND active;

DROP TABLE projection_expression_04839;
