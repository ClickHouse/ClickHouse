-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS multiarray_lambda_param_04839;

-- arrayMap((x, y) -> y, arr, meta_arr): only meta_arr (bound to y, used in the body) is a value
-- donor for the output; arr (bound to x, unused) must not donate its rule despite being an argument.
CREATE TABLE multiarray_lambda_param_04839
(
    id UInt64,
    arr Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_a')),
    meta_arr Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_b'))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO multiarray_lambda_param_04839 VALUES (1, ['{"tag_a1":1}'], ['{"tag_b1":2}']);

ALTER TABLE multiarray_lambda_param_04839 MODIFY COLUMN arr Array(JSON(max_dynamic_paths=5));
ALTER TABLE multiarray_lambda_param_04839 MODIFY COLUMN meta_arr Array(JSON(max_dynamic_paths=5));

ALTER TABLE multiarray_lambda_param_04839
    ADD PROJECTION p (SELECT id, arrayMap((x, y) -> y, arr, meta_arr) WHERE id > 0 ORDER BY id);
ALTER TABLE multiarray_lambda_param_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

-- The regression: only tag_b (meta_arr's rule) may appear; tag_a (arr's rule, unused param) must not.
SELECT
    'unused-parameter array does not donate provenance',
    countIf(position(type, '^tag_b') > 0 AND position(type, '^tag_a') = 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='multiarray_lambda_param_04839' AND column != 'id' AND active;

DROP TABLE multiarray_lambda_param_04839;
