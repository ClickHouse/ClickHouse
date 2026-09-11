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
    'materialize(tuple(j1,j2)) type',
    type
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='wrapped_tuple_04839' AND column != 'id' AND active;

DROP TABLE wrapped_tuple_04839;

DROP TABLE IF EXISTS wrapped_lambda_body_04839;

-- The lambda body may also sit under a transparent wrapper: arrayMap((x, y) -> materialize(tuple(x, y)), ...)
-- and mapApply((k, v) -> materialize(tuple(k, v)), m) must still get the slot-wise handling.
CREATE TABLE wrapped_lambda_body_04839
(
    id UInt64,
    arr1 Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^a_')),
    arr2 Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^b_')),
    m Map(String, JSON(max_dynamic_paths=5, SHARED REGEXP '^c_'))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO wrapped_lambda_body_04839 VALUES (1, ['{"a_x":1}'], ['{"b_x":2}'], {'k':'{"c_x":3}'});

ALTER TABLE wrapped_lambda_body_04839
    MODIFY COLUMN arr1 Array(JSON(max_dynamic_paths=5)),
    MODIFY COLUMN arr2 Array(JSON(max_dynamic_paths=5)),
    MODIFY COLUMN m Map(String, JSON(max_dynamic_paths=5));

ALTER TABLE wrapped_lambda_body_04839
    ADD PROJECTION p (SELECT id, arrayMap((x, y) -> materialize(tuple(x, y)), arr1, arr2) WHERE id > 0 ORDER BY id);
ALTER TABLE wrapped_lambda_body_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

ALTER TABLE wrapped_lambda_body_04839
    ADD PROJECTION pm (SELECT id, mapApply((k, v) -> materialize(tuple(k, v)), m) WHERE id > 0 ORDER BY id);
ALTER TABLE wrapped_lambda_body_04839 MATERIALIZE PROJECTION pm SETTINGS mutations_sync=1;

SELECT 'arrayMap materialize(tuple(x,y)) slots carry their own policies',
       type
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='wrapped_lambda_body_04839' AND name = 'p' AND column LIKE 'arrayMap%' AND active;

SELECT 'mapApply materialize(tuple(k,v)) values carry their policy',
       countIf(position(type, '^c_') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='wrapped_lambda_body_04839' AND name = 'pm' AND column LIKE 'mapApply%' AND active;

DROP TABLE wrapped_lambda_body_04839;
