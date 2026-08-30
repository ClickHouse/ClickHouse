-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

-- tuple(j1,j2)/map(j1,j2)/arrayZip(j1,j2) feed different structural slots from different sources;
-- merging each against the whole result type drops both (tuple) or unions them onto both sides (map).

DROP TABLE IF EXISTS tuple_two_json_04839;
CREATE TABLE tuple_two_json_04839
(
    id UInt64,
    j1 JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_a'),
    j2 JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_b')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO tuple_two_json_04839 VALUES (1, '{"tag_a1":1}', '{"tag_b1":2}');

ALTER TABLE tuple_two_json_04839 MODIFY COLUMN j1 JSON(max_dynamic_paths=5);
ALTER TABLE tuple_two_json_04839 MODIFY COLUMN j2 JSON(max_dynamic_paths=5);

ALTER TABLE tuple_two_json_04839 ADD PROJECTION p (SELECT id, tuple(j1, j2) WHERE id > 0 ORDER BY id);
ALTER TABLE tuple_two_json_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

-- Each element must keep only its own rule: print the type so a union onto one element, a copy
-- onto both, or a swap is visible, not only the case where both rules disappear.
SELECT
    'tuple(j1,j2) type',
    type
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='tuple_two_json_04839' AND column='tuple(j1, j2)' AND active;

DROP TABLE tuple_two_json_04839;

DROP TABLE IF EXISTS map_two_json_04839;
CREATE TABLE map_two_json_04839
(
    id UInt64,
    j1 JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_a'),
    j2 JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_b')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO map_two_json_04839 VALUES (1, '{"tag_a1":1}', '{"tag_b1":2}');

ALTER TABLE map_two_json_04839 MODIFY COLUMN j1 JSON(max_dynamic_paths=5);
ALTER TABLE map_two_json_04839 MODIFY COLUMN j2 JSON(max_dynamic_paths=5);

ALTER TABLE map_two_json_04839 ADD PROJECTION p (SELECT id, map(j1, j2) WHERE id > 0 ORDER BY id);
ALTER TABLE map_two_json_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

-- key must keep only j1's rule and value only j2's -- not unioned onto both sides.
SELECT
    'map(j1,j2) type',
    type
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='map_two_json_04839' AND column='map(j1, j2)' AND active;

DROP TABLE map_two_json_04839;

DROP TABLE IF EXISTS arrayzip_two_json_04839;
CREATE TABLE arrayzip_two_json_04839
(
    id UInt64,
    arr1 Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_a')),
    arr2 Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_b'))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO arrayzip_two_json_04839 VALUES (1, ['{"tag_a1":1}'], ['{"tag_b1":2}']);

ALTER TABLE arrayzip_two_json_04839 MODIFY COLUMN arr1 Array(JSON(max_dynamic_paths=5));
ALTER TABLE arrayzip_two_json_04839 MODIFY COLUMN arr2 Array(JSON(max_dynamic_paths=5));

ALTER TABLE arrayzip_two_json_04839 ADD PROJECTION p (SELECT id, arrayZip(arr1, arr2) WHERE id > 0 ORDER BY id);
ALTER TABLE arrayzip_two_json_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

-- Same as tuple(j1,j2), just wrapped in Array: each element keeps only its own rule.
SELECT
    'arrayZip(arr1,arr2) type',
    type
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='arrayzip_two_json_04839' AND column='arrayZip(arr1, arr2)' AND active;

DROP TABLE arrayzip_two_json_04839;

DROP TABLE IF EXISTS arrayzipunaligned_two_json_04839;
CREATE TABLE arrayzipunaligned_two_json_04839
(
    id UInt64,
    arr1 Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_a')),
    arr2 Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_b'))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO arrayzipunaligned_two_json_04839 VALUES (1, ['{"tag_a1":1}'], ['{"tag_b1":2}']);

ALTER TABLE arrayzipunaligned_two_json_04839 MODIFY COLUMN arr1 Array(JSON(max_dynamic_paths=5));
ALTER TABLE arrayzipunaligned_two_json_04839 MODIFY COLUMN arr2 Array(JSON(max_dynamic_paths=5));

ALTER TABLE arrayzipunaligned_two_json_04839 ADD PROJECTION p (SELECT id, arrayZipUnaligned(arr1, arr2) WHERE id > 0 ORDER BY id);
ALTER TABLE arrayzipunaligned_two_json_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

-- arrayZipUnaligned nullable-wraps each slot; print the type so a crossed or dropped rule is visible.
SELECT
    'arrayZipUnaligned(arr1,arr2) type',
    type
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='arrayzipunaligned_two_json_04839' AND column='arrayZipUnaligned(arr1, arr2)' AND active;

DROP TABLE arrayzipunaligned_two_json_04839;

-- A single aligned donor must still resolve a multi-JSON output: arrayElement(arr, 1) and a
-- nullability wrapper over a whole tuple are one-source shapes the ambiguity guard used to drop.
DROP TABLE IF EXISTS single_donor_two_json_04839;
CREATE TABLE single_donor_two_json_04839
(
    id UInt64,
    arr Array(Tuple(a JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_a'), b JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_b'))),
    t Tuple(a JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_a'), b JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_b'))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO single_donor_two_json_04839 VALUES (1, [('{"tag_a1":1}', '{"tag_b1":2}')], ('{"tag_a1":1}', '{"tag_b1":2}'));

ALTER TABLE single_donor_two_json_04839
    MODIFY COLUMN arr Array(Tuple(a JSON(max_dynamic_paths=5), b JSON(max_dynamic_paths=5))),
    MODIFY COLUMN t Tuple(a JSON(max_dynamic_paths=5), b JSON(max_dynamic_paths=5));

ALTER TABLE single_donor_two_json_04839 ADD PROJECTION p (SELECT id, arrayElement(arr, 1), assumeNotNull(t) WHERE id > 0 ORDER BY id);
ALTER TABLE single_donor_two_json_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

SELECT
    'single aligned donor',
    column,
    type
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='single_donor_two_json_04839' AND active
    AND column IN ('arrayElement(arr, 1)', 'assumeNotNull(t)')
ORDER BY column;

DROP TABLE single_donor_two_json_04839;

-- A conditional picks one branch per row, so slot i of the result is fed by slot i of every branch.
-- Without slot-wise handling the flat candidate list reaches the self-only fallback, which drops
-- every donor once the output holds more than one JSON node, and both rules are lost.
DROP TABLE IF EXISTS if_two_json_04839;
CREATE TABLE if_two_json_04839
(
    id UInt64,
    j1 JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_a'),
    j2 JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_b'),
    k1 JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_a'),
    k2 JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_b')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO if_two_json_04839 VALUES (1, '{"tag_a1":1}', '{"tag_b1":2}', '{"tag_a2":3}', '{"tag_b2":4}');

ALTER TABLE if_two_json_04839 MODIFY COLUMN j1 JSON(max_dynamic_paths=5);
ALTER TABLE if_two_json_04839 MODIFY COLUMN j2 JSON(max_dynamic_paths=5);
ALTER TABLE if_two_json_04839 MODIFY COLUMN k1 JSON(max_dynamic_paths=5);
ALTER TABLE if_two_json_04839 MODIFY COLUMN k2 JSON(max_dynamic_paths=5);

ALTER TABLE if_two_json_04839 ADD PROJECTION p (SELECT id, if(id > 0, tuple(j1, j2), tuple(k1, k2)) WHERE id > 0 ORDER BY id);
ALTER TABLE if_two_json_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

-- Both branches carry the same rule in each slot, so the slot-wise union is unambiguous.
SELECT
    'if(tuple,tuple) type',
    type
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='if_two_json_04839' AND column LIKE 'if(%' AND active;

DROP TABLE if_two_json_04839;

-- The branches do not have to be bare tuples: map and arrayZip rebuild the output slot-wise too,
-- so a conditional over them has to recurse through the same structural handling. Without it the
-- flat list reaches the self-only fallback and both rules are lost, exactly as for tuple branches.
DROP TABLE IF EXISTS if_map_json_04839;
CREATE TABLE if_map_json_04839
(
    id UInt64,
    j1 JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_a'),
    j2 JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_b'),
    k1 JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_a'),
    k2 JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_b')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO if_map_json_04839 VALUES (1, '{"tag_a1":1}', '{"tag_b1":2}', '{"tag_a2":3}', '{"tag_b2":4}');

ALTER TABLE if_map_json_04839 MODIFY COLUMN j1 JSON(max_dynamic_paths=5);
ALTER TABLE if_map_json_04839 MODIFY COLUMN j2 JSON(max_dynamic_paths=5);
ALTER TABLE if_map_json_04839 MODIFY COLUMN k1 JSON(max_dynamic_paths=5);
ALTER TABLE if_map_json_04839 MODIFY COLUMN k2 JSON(max_dynamic_paths=5);

ALTER TABLE if_map_json_04839 ADD PROJECTION p (SELECT id, if(id > 0, map(j1, j2), map(k1, k2)) WHERE id > 0 ORDER BY id);
ALTER TABLE if_map_json_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

SELECT
    'if(map,map) type',
    type
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='if_map_json_04839' AND column LIKE 'if(%' AND active;

DROP TABLE if_map_json_04839;

DROP TABLE IF EXISTS if_arrayzip_json_04839;
CREATE TABLE if_arrayzip_json_04839
(
    id UInt64,
    arr1 Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_a')),
    arr2 Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_b')),
    arr3 Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_a')),
    arr4 Array(JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_b'))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO if_arrayzip_json_04839 VALUES (1, ['{"tag_a1":1}'], ['{"tag_b1":2}'], ['{"tag_a2":3}'], ['{"tag_b2":4}']);

ALTER TABLE if_arrayzip_json_04839 MODIFY COLUMN arr1 Array(JSON(max_dynamic_paths=5));
ALTER TABLE if_arrayzip_json_04839 MODIFY COLUMN arr2 Array(JSON(max_dynamic_paths=5));
ALTER TABLE if_arrayzip_json_04839 MODIFY COLUMN arr3 Array(JSON(max_dynamic_paths=5));
ALTER TABLE if_arrayzip_json_04839 MODIFY COLUMN arr4 Array(JSON(max_dynamic_paths=5));

ALTER TABLE if_arrayzip_json_04839 ADD PROJECTION p (SELECT id, if(id > 0, arrayZip(arr1, arr2), arrayZip(arr3, arr4)) WHERE id > 0 ORDER BY id);
ALTER TABLE if_arrayzip_json_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

SELECT
    'if(arrayZip,arrayZip) type',
    type
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='if_arrayzip_json_04839' AND column LIKE 'if(%' AND active;

DROP TABLE if_arrayzip_json_04839;
