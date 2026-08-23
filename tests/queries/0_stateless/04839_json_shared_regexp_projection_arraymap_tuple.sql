-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS arraymap_tuple_04839;

-- arrayMap((x, y) -> tuple(x, y), arr1, arr2) builds the tuple element-wise, so each slot must
-- retain its own source's policy and never its sibling's.
CREATE TABLE arraymap_tuple_04839
(
    id UInt64,
    arr1 Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^a_')),
    arr2 Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^b_'))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO arraymap_tuple_04839 VALUES (1, ['{"a_x":1}'], ['{"b_x":2}']);

ALTER TABLE arraymap_tuple_04839
    MODIFY COLUMN arr1 Array(JSON(max_dynamic_paths=5)),
    MODIFY COLUMN arr2 Array(JSON(max_dynamic_paths=5));

ALTER TABLE arraymap_tuple_04839
    ADD PROJECTION p (SELECT id, arrayMap((x, y) -> tuple(x, y), arr1, arr2) WHERE id > 0 ORDER BY id);
ALTER TABLE arraymap_tuple_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

SELECT 'slots carry their own policies',
       countIf(position(type, '^a_') > 0 AND position(type, '^b_') > 0 AND position(type, '^a_') < position(type, '^b_'))
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='arraymap_tuple_04839' AND name = 'p' AND column LIKE 'arrayMap%' AND active;

DROP TABLE arraymap_tuple_04839;

-- assumeNotNull is provenance-transparent like materialize: the lambda's array source binding must
-- still see `arr` through it, or the qualified `x.doc` donor is lost and the rule dropped.
DROP TABLE IF EXISTS arraymap_notnull_04839;
CREATE TABLE arraymap_notnull_04839
(
    id UInt64,
    arr Array(Tuple(doc JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_'), n UInt8))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO arraymap_notnull_04839 VALUES (1, [('{"tag_x":1}', 1)]);

ALTER TABLE arraymap_notnull_04839 MODIFY COLUMN arr Array(Tuple(doc JSON(max_dynamic_paths=5), n UInt8));

ALTER TABLE arraymap_notnull_04839
    ADD PROJECTION p (SELECT id, arrayMap(x -> tuple(tupleElement(x, 'doc'), 1), assumeNotNull(arr)) WHERE id > 0 ORDER BY id);
ALTER TABLE arraymap_notnull_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

SELECT 'assumeNotNull-wrapped source keeps the doc rule',
       countIf(position(type, '^tag_') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='arraymap_notnull_04839' AND name = 'p' AND column LIKE 'arrayMap%' AND active;

DROP TABLE arraymap_notnull_04839;
