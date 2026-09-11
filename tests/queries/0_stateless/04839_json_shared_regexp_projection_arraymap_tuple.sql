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

-- Print the type: a union of both rules onto one slot, or a copy onto both, still satisfies a
-- presence-and-order check but breaks the element-wise contract above.
SELECT 'slots carry their own policies',
       type
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='arraymap_tuple_04839' AND name = 'p' AND column LIKE 'arrayMap%' AND active;

DROP TABLE arraymap_tuple_04839;

-- The lambda body does not have to be a tuple: arrayMap((k, v) -> map(k, v), ...) builds
-- Array(Map(...)) element-wise the same way, so the key and value sides must keep their own
-- policies rather than both being dropped as an ambiguous multi-JSON output.
DROP TABLE IF EXISTS arraymap_map_04839;
CREATE TABLE arraymap_map_04839
(
    id UInt64,
    arr1 Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^a_')),
    arr2 Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^b_'))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO arraymap_map_04839 VALUES (1, ['{"a_x":1}'], ['{"b_x":2}']);

ALTER TABLE arraymap_map_04839
    MODIFY COLUMN arr1 Array(JSON(max_dynamic_paths=5)),
    MODIFY COLUMN arr2 Array(JSON(max_dynamic_paths=5));

ALTER TABLE arraymap_map_04839
    ADD PROJECTION p (SELECT id, arrayMap((k, v) -> map(k, v), arr1, arr2) WHERE id > 0 ORDER BY id);
ALTER TABLE arraymap_map_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

SELECT 'map key and value carry their own policies',
       type
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='arraymap_map_04839' AND name = 'p' AND column LIKE 'arrayMap%' AND active;

DROP TABLE arraymap_map_04839;

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

-- A transparent wrapper *inside* a member access must not hide the member: mapValues(materialize(m))
-- names `m.values`, not the whole `m`, whose Map shape no longer aligns with the Array(JSON) output.
DROP TABLE IF EXISTS mapvalues_wrapped_04839;
CREATE TABLE mapvalues_wrapped_04839
(
    id UInt64,
    m Map(String, JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_'))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO mapvalues_wrapped_04839 VALUES (1, {'k': '{"tag_x":1}'});

ALTER TABLE mapvalues_wrapped_04839 MODIFY COLUMN m Map(String, JSON(max_dynamic_paths=5));

ALTER TABLE mapvalues_wrapped_04839
    ADD PROJECTION p (SELECT id, mapValues(materialize(m)) WHERE id > 0 ORDER BY id);
ALTER TABLE mapvalues_wrapped_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

SELECT 'wrapped member base keeps the values rule',
       countIf(position(type, '^tag_') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='mapvalues_wrapped_04839' AND name = 'p' AND column LIKE 'mapValues%' AND active;

DROP TABLE mapvalues_wrapped_04839;

-- A scalar lambda output reads one member of the bound element, so the donor is `arr.doc`. Donating
-- the whole `arr` would leave two JSON siblings to choose from and drop the rule instead.
DROP TABLE IF EXISTS arraymap_scalar_member_04839;
CREATE TABLE arraymap_scalar_member_04839
(
    id UInt64,
    arr Array(Tuple(doc JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_'), other JSON(max_dynamic_paths=5, SHARED REGEXP '^oth_')))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO arraymap_scalar_member_04839 VALUES (1, [('{"tag_x":1}', '{"oth_y":2}')]);

ALTER TABLE arraymap_scalar_member_04839
    MODIFY COLUMN arr Array(Tuple(doc JSON(max_dynamic_paths=5), other JSON(max_dynamic_paths=5)));

ALTER TABLE arraymap_scalar_member_04839
    ADD PROJECTION p (SELECT id, arrayMap(x -> tupleElement(x, 'doc'), arr) WHERE id > 0 ORDER BY id);
ALTER TABLE arraymap_scalar_member_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

SELECT 'scalar lambda output keeps only its own member rule',
       countIf(position(type, '^tag_') > 0 AND position(type, '^oth_') = 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='arraymap_scalar_member_04839' AND name = 'p' AND column LIKE 'arrayMap%' AND active;

DROP TABLE arraymap_scalar_member_04839;

-- The bound source of a slot-wise reconstruction can itself be member-qualified: `tupleElement(t, 'arr')`
-- must contribute `t.arr.doc`, not be dropped for failing a bare-identifier check.
DROP TABLE IF EXISTS arraymap_qualified_source_04839;
CREATE TABLE arraymap_qualified_source_04839
(
    id UInt64,
    t Tuple(arr Array(Tuple(doc JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_'), n UInt8)))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO arraymap_qualified_source_04839 VALUES (1, ([('{"tag_x":1}', 1)]));

ALTER TABLE arraymap_qualified_source_04839
    MODIFY COLUMN t Tuple(arr Array(Tuple(doc JSON(max_dynamic_paths=5), n UInt8)));

ALTER TABLE arraymap_qualified_source_04839
    ADD PROJECTION p (SELECT id, arrayMap(x -> tuple(tupleElement(x, 'doc'), 1), tupleElement(t, 'arr')) WHERE id > 0 ORDER BY id);
ALTER TABLE arraymap_qualified_source_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

SELECT 'member-qualified lambda source keeps the doc rule',
       countIf(position(type, '^tag_') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='arraymap_qualified_source_04839' AND name = 'p' AND column LIKE 'arrayMap%' AND active;

DROP TABLE arraymap_qualified_source_04839;

-- mapApply has its own member-qualified source binding: `tupleElement(t, 'm')` must contribute
-- `t.m.keys` / `t.m.values`, not fail the bare-identifier check and drop the value slot's rule.
DROP TABLE IF EXISTS mapapply_qualified_source_04839;
CREATE TABLE mapapply_qualified_source_04839
(
    id UInt64,
    t Tuple(m Map(String, Tuple(doc JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_'), n UInt8)))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO mapapply_qualified_source_04839 VALUES (1, tuple(map('left', ('{"tag_x":1}', 1))));

ALTER TABLE mapapply_qualified_source_04839
    MODIFY COLUMN t Tuple(m Map(String, Tuple(doc JSON(max_dynamic_paths=5), n UInt8)));

ALTER TABLE mapapply_qualified_source_04839
    ADD PROJECTION p (SELECT id, mapApply((k, v) -> tuple(k, tupleElement(v, 'doc')), tupleElement(t, 'm')) WHERE id > 0 ORDER BY id);
ALTER TABLE mapapply_qualified_source_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

SELECT 'member-qualified mapApply source keeps the doc rule',
       countIf(position(type, '^tag_') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='mapapply_qualified_source_04839' AND name = 'p' AND column LIKE 'mapApply%' AND active;

DROP TABLE mapapply_qualified_source_04839;
