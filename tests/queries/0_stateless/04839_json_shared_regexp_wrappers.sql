-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS wrappers_04839;

CREATE TABLE wrappers_04839
(
    id UInt64,
    arr Array(JSON(max_dynamic_paths=1, SHARED REGEXP '^force$')),
    tup Tuple(doc JSON(max_dynamic_paths=1, SHARED REGEXP '^force$')),
    mp Map(String, JSON(max_dynamic_paths=1, SHARED REGEXP '^force$')),
    nul Nullable(JSON(max_dynamic_paths=1, SHARED REGEXP '^force$'))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part=0,
    min_bytes_for_wide_part=0,
    max_bytes_to_merge_at_max_space_in_pool=1,
    map_serialization_version='basic',
    map_serialization_version_for_zero_level_parts='basic';

SYSTEM STOP MERGES wrappers_04839;

INSERT INTO wrappers_04839 VALUES
(
    1,
    ['{"force":1,"keep":11}', '{"force":2}'],
    tuple('{"force":3,"keep":13}'),
    map('left', '{"force":4,"keep":14}', 'right', '{"force":5}'),
    '{"force":20,"keep":21}'
);

INSERT INTO wrappers_04839 VALUES
(
    2,
    ['{"force":6,"keep":16}', '{"force":7}'],
    tuple('{"force":8,"keep":18}'),
    map('left', '{"force":9,"keep":19}', 'right', '{"force":10}'),
    NULL
);

-- Read each part on its own: a read that merges both parts rebuilds the placement of the result.
SELECT
    'before alter',
    id,
    arrayMap(x -> arraySort(JSONDynamicPaths(x)), arr),
    arrayMap(x -> arraySort(JSONSharedDataPaths(x)), arr),
    arraySort(JSONDynamicPaths(tup.doc)),
    arraySort(JSONSharedDataPaths(tup.doc)),
    arrayMap(x -> arraySort(JSONDynamicPaths(x)), mapValues(mp)),
    arrayMap(x -> arraySort(JSONSharedDataPaths(x)), mapValues(mp)),
    arraySort(JSONDynamicPaths(nul)),
    arraySort(JSONSharedDataPaths(nul))
FROM wrappers_04839
WHERE id = 1;

SELECT
    'before alter',
    id,
    arrayMap(x -> arraySort(JSONDynamicPaths(x)), arr),
    arrayMap(x -> arraySort(JSONSharedDataPaths(x)), arr),
    arraySort(JSONDynamicPaths(tup.doc)),
    arraySort(JSONSharedDataPaths(tup.doc)),
    arrayMap(x -> arraySort(JSONDynamicPaths(x)), mapValues(mp)),
    arrayMap(x -> arraySort(JSONSharedDataPaths(x)), mapValues(mp)),
    arraySort(JSONDynamicPaths(nul)),
    arraySort(JSONSharedDataPaths(nul))
FROM wrappers_04839
WHERE id = 2;

SELECT 'values before', id, arr, tup, mp, nul FROM wrappers_04839 ORDER BY id;

-- A rules-only change of JSON inside Array or Nullable is metadata-only.
ALTER TABLE wrappers_04839
    MODIFY COLUMN arr Array(JSON(max_dynamic_paths=1)),
    MODIFY COLUMN nul Nullable(JSON(max_dynamic_paths=1));

SELECT 'mutations after arr and nul', count() FROM system.mutations WHERE database=currentDatabase() AND table='wrappers_04839';

SELECT
    'after arr and nul alter',
    id,
    arrayMap(x -> arraySort(JSONDynamicPaths(x)), arr),
    arrayMap(x -> arraySort(JSONSharedDataPaths(x)), arr),
    arraySort(JSONDynamicPaths(tup.doc)),
    arraySort(JSONSharedDataPaths(tup.doc)),
    arrayMap(x -> arraySort(JSONDynamicPaths(x)), mapValues(mp)),
    arrayMap(x -> arraySort(JSONSharedDataPaths(x)), mapValues(mp)),
    arraySort(JSONDynamicPaths(nul)),
    arraySort(JSONSharedDataPaths(nul))
FROM wrappers_04839
WHERE id = 1;

SELECT
    'after arr and nul alter',
    id,
    arrayMap(x -> arraySort(JSONDynamicPaths(x)), arr),
    arrayMap(x -> arraySort(JSONSharedDataPaths(x)), arr),
    arraySort(JSONDynamicPaths(tup.doc)),
    arraySort(JSONSharedDataPaths(tup.doc)),
    arrayMap(x -> arraySort(JSONDynamicPaths(x)), mapValues(mp)),
    arrayMap(x -> arraySort(JSONSharedDataPaths(x)), mapValues(mp)),
    arraySort(JSONDynamicPaths(nul)),
    arraySort(JSONSharedDataPaths(nul))
FROM wrappers_04839
WHERE id = 2;

-- Inside Tuple or Map the change is a regular type conversion that rewrites the column.
SYSTEM START MERGES wrappers_04839;

ALTER TABLE wrappers_04839
    MODIFY COLUMN tup Tuple(doc JSON(max_dynamic_paths=1)),
    MODIFY COLUMN mp Map(String, JSON(max_dynamic_paths=1))
SETTINGS mutations_sync=2;

SELECT 'mutations after tup and mp', count() > 0 FROM system.mutations WHERE database=currentDatabase() AND table='wrappers_04839';

SELECT 'values after alter', id, arr, tup, mp, nul FROM wrappers_04839 ORDER BY id;

OPTIMIZE TABLE wrappers_04839 FINAL;

SELECT column, type
FROM system.parts_columns
WHERE database=currentDatabase() AND table='wrappers_04839' AND active AND column IN ('arr', 'tup', 'mp', 'nul')
ORDER BY column;

SELECT
    'after merge',
    id,
    arrayMap(x -> arraySort(JSONDynamicPaths(x)), arr),
    arrayMap(x -> arraySort(JSONSharedDataPaths(x)), arr),
    arraySort(JSONDynamicPaths(tup.doc)),
    arraySort(JSONSharedDataPaths(tup.doc)),
    arrayMap(x -> arraySort(JSONDynamicPaths(x)), mapValues(mp)),
    arrayMap(x -> arraySort(JSONSharedDataPaths(x)), mapValues(mp)),
    arraySort(JSONDynamicPaths(nul)),
    arraySort(JSONSharedDataPaths(nul))
FROM wrappers_04839
ORDER BY id;

SELECT 'values after merge', id, arr, tup, mp, nul FROM wrappers_04839 ORDER BY id;

DROP TABLE wrappers_04839;

-- Changing JSON inside Variant is rejected, as for any other JSON parameter.
SET enable_variant_type = 1;

DROP TABLE IF EXISTS variant_wrappers_04839;

CREATE TABLE variant_wrappers_04839
(
    id UInt64,
    var Variant(JSON(max_dynamic_paths=1, SHARED REGEXP '^force$'), UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0;

ALTER TABLE variant_wrappers_04839 MODIFY COLUMN var Variant(JSON(max_dynamic_paths=1, SHARED REGEXP '^other$'), UInt64); -- { serverError CANNOT_CONVERT_TYPE }

DROP TABLE variant_wrappers_04839;
