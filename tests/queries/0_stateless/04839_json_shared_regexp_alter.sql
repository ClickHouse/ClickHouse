-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS alter_04839;

CREATE TABLE alter_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=2, SHARED REGEXP '^force$')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0;

SYSTEM STOP MERGES alter_04839;

INSERT INTO alter_04839 SELECT number, toJSONString(map('force', number, 'keep', number)) FROM numbers(4);
INSERT INTO alter_04839 SELECT number + 4, toJSONString(map('force', number + 4)) FROM numbers(4);

SELECT
    'initial',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    sum(cityHash64(toJSONString(j)))
FROM alter_04839;

-- Removing the rule is metadata-only: old parts keep their type on disk, and reads convert them to the current type.
ALTER TABLE alter_04839 MODIFY COLUMN j JSON(max_dynamic_paths=2);

SELECT 'mutations', count() FROM system.mutations WHERE database=currentDatabase() AND table='alter_04839';

SELECT
    'removed',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    sum(cityHash64(toJSONString(j)))
FROM alter_04839;

DETACH TABLE alter_04839;
ATTACH TABLE alter_04839;
SYSTEM STOP MERGES alter_04839;

SELECT
    'reattached types',
    (SELECT type FROM system.columns WHERE database=currentDatabase() AND table='alter_04839' AND name='j'),
    arraySort(groupArray(type))
FROM system.parts_columns
WHERE database=currentDatabase() AND table='alter_04839' AND column='j' AND active;

SELECT
    'reattached',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    sum(cityHash64(toJSONString(j)))
FROM alter_04839;

-- The next merge uses the current type, so the frequent path formerly kept in shared data is promoted.
SYSTEM START MERGES alter_04839;
OPTIMIZE TABLE alter_04839 FINAL;

SELECT
    'merged without rule types',
    (SELECT type FROM system.columns WHERE database=currentDatabase() AND table='alter_04839' AND name='j'),
    arraySort(groupArray(type))
FROM system.parts_columns
WHERE database=currentDatabase() AND table='alter_04839' AND column='j' AND active;

SELECT
    'merged without rule',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    sum(cityHash64(toJSONString(j)))
FROM alter_04839;

-- Adding the rule back is metadata-only too, and reads already move the path back to shared data.
ALTER TABLE alter_04839 MODIFY COLUMN j JSON(max_dynamic_paths=2, SHARED REGEXP '^force$');

SELECT 'mutations', count() FROM system.mutations WHERE database=currentDatabase() AND table='alter_04839';

SELECT
    'added',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    sum(cityHash64(toJSONString(j)))
FROM alter_04839;

OPTIMIZE TABLE alter_04839 FINAL;

SELECT
    'merged with rule types',
    (SELECT type FROM system.columns WHERE database=currentDatabase() AND table='alter_04839' AND name='j'),
    arraySort(groupArray(type))
FROM system.parts_columns
WHERE database=currentDatabase() AND table='alter_04839' AND column='j' AND active;

SELECT
    'merged with rule',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    sum(cityHash64(toJSONString(j)))
FROM alter_04839;

DROP TABLE alter_04839;
