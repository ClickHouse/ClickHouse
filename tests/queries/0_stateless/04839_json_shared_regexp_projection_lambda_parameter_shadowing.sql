-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS lambda_parameter_shadowing_04839;

-- A lambda's bound parameter must be masked like RequiredSourceColumnsVisitor masks it, or it can
-- collide with a same-named real column and donate that column's SHARED REGEXP rule to the wrong output.
CREATE TABLE lambda_parameter_shadowing_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_'),
    arr Array(JSON(max_dynamic_paths=5))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO lambda_parameter_shadowing_04839 VALUES (1, '{"tag_a":1}', ['{"keep":1}']);

ALTER TABLE lambda_parameter_shadowing_04839
    ADD PROJECTION p (SELECT id, arrayMap(j -> j, arr) AS mapped WHERE id > 0 ORDER BY id);
ALTER TABLE lambda_parameter_shadowing_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

-- The regression: this must be 0. The lambda's bound parameter `j` must not be resolved against
-- the unrelated top-level `j` column, whose SHARED REGEXP rule `mapped` has nothing to do with.
SELECT
    'lambda parameter shadowing provenance',
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='lambda_parameter_shadowing_04839' AND column != 'id' AND active;

DROP TABLE lambda_parameter_shadowing_04839;
