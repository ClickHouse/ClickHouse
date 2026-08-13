-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS projection_expression_map_04839;

-- Same wrapper-mismatch class as the array(j)/toNullable(j) tests, but for a projection expression
-- that wraps its argument as a Map value (map('k', j)); the JSON value only ever lives in the
-- value type, so the policy needs to descend there specifically, leaving the key type untouched.
CREATE TABLE projection_expression_map_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO projection_expression_map_04839 VALUES (1, '{"tag_a":1,"keep":1}');

-- Retire the rule at the table level; the existing part's own j type still carries it as history.
ALTER TABLE projection_expression_map_04839 MODIFY COLUMN j JSON(max_dynamic_paths=5);

ALTER TABLE projection_expression_map_04839
    ADD PROJECTION p (SELECT id, map('k', j) WHERE id > 0 ORDER BY id);
ALTER TABLE projection_expression_map_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=2;

-- Control: the projection's own column really is named after the expression and Map-wrapped.
SELECT
    'projection column name and type',
    countIf(column = 'map(\'k\', j)' AND startsWith(type, 'Map'))
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='projection_expression_map_04839' AND part_name='p' AND active;

-- The regression: this must be 1.
SELECT
    'projection expression map provenance',
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='projection_expression_map_04839' AND column='map(\'k\', j)' AND active;

DROP TABLE projection_expression_map_04839;
