-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS projection_expression_map_value_04839;

-- Source-side mirror of 04839_json_shared_regexp_projection_expression_map.sql: there the
-- projection expression added the Map wrapper the source column never had; here the base column
-- itself is the Map, and the projection expression (m['k']) unwraps it back to bare JSON.
CREATE TABLE projection_expression_map_value_04839
(
    id UInt64,
    m Map(String, JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_'))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO projection_expression_map_value_04839 VALUES (1, map('k', '{"tag_a":1,"keep":1}'));

-- Retire the rule at the table level; the existing part's own m type still carries it as history.
ALTER TABLE projection_expression_map_value_04839 MODIFY COLUMN m Map(String, JSON(max_dynamic_paths=5));

ALTER TABLE projection_expression_map_value_04839
    ADD PROJECTION p (SELECT id, m['k'] WHERE id > 0 ORDER BY id);
ALTER TABLE projection_expression_map_value_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=2;

-- Control: the projection's own column really is named after the expression (ClickHouse
-- canonicalizes m['k'] to arrayElement(m, 'k'), matching Map's underlying representation) and
-- bare (not Map).
SELECT
    'projection column name and type',
    countIf(column = 'arrayElement(m, \'k\')' AND NOT startsWith(type, 'Map'))
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='projection_expression_map_value_04839' AND part_name='p' AND active;

-- The regression: this must be 1.
SELECT
    'projection expression map value provenance',
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='projection_expression_map_value_04839' AND column='arrayElement(m, \'k\')' AND active;

DROP TABLE projection_expression_map_value_04839;
