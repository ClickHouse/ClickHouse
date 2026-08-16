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

-- Both rules must survive, each on its own tuple element -- not both dropped by the ambiguity guard.
SELECT
    'tuple(j1,j2) retains both elements'' own provenance',
    countIf(position(type, '^tag_a') > 0 AND position(type, '^tag_b') > 0)
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

-- Same as tuple(j1,j2), just wrapped in Array: both rules must survive on their own element.
SELECT
    'arrayZip(arr1,arr2) retains both elements'' own provenance',
    countIf(position(type, '^tag_a') > 0 AND position(type, '^tag_b') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='arrayzip_two_json_04839' AND column='arrayZip(arr1, arr2)' AND active;

DROP TABLE arrayzip_two_json_04839;
