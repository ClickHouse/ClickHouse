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
ORDER BY id;

-- Policy-only comparison must recurse through all four wrappers, so this ALTER is metadata-only.
ALTER TABLE wrappers_04839
    MODIFY COLUMN arr Array(JSON(max_dynamic_paths=1)),
    MODIFY COLUMN tup Tuple(doc JSON(max_dynamic_paths=1)),
    MODIFY COLUMN mp Map(String, JSON(max_dynamic_paths=1)),
    MODIFY COLUMN nul Nullable(JSON(max_dynamic_paths=1));

SELECT
    count() AS active_parts,
    (SELECT count() FROM system.mutations
     WHERE database=currentDatabase() AND table='wrappers_04839') AS mutations
FROM system.parts
WHERE database=currentDatabase() AND table='wrappers_04839' AND active;

DETACH TABLE wrappers_04839;
ATTACH TABLE wrappers_04839;
SYSTEM STOP MERGES wrappers_04839;

SELECT name, position(type, 'SHARED REGEXP') > 0
FROM system.columns
WHERE database=currentDatabase() AND table='wrappers_04839' AND name IN ('arr', 'tup', 'mp', 'nul')
ORDER BY name;

SELECT
    column,
    count(),
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.parts_columns
WHERE database=currentDatabase() AND table='wrappers_04839' AND active AND column IN ('arr', 'tup', 'mp', 'nul')
GROUP BY column
ORDER BY column;

SELECT
    'after metadata reload',
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
WHERE id = 1
ORDER BY id;

SELECT
    'after metadata reload',
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
WHERE id = 2
ORDER BY id;

SYSTEM START MERGES wrappers_04839;
OPTIMIZE TABLE wrappers_04839 FINAL;

SELECT
    column,
    count(),
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.parts_columns
WHERE database=currentDatabase() AND table='wrappers_04839' AND active AND column IN ('arr', 'tup', 'mp', 'nul')
GROUP BY column
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

DROP TABLE wrappers_04839;
