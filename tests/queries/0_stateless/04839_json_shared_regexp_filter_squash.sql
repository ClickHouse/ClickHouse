-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings

SET enable_json_type = 1;

DROP TABLE IF EXISTS source_04839;
DROP TABLE IF EXISTS destination_04839;

CREATE TABLE source_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=10, SHARED REGEXP '^force$')
)
ENGINE = Memory;

CREATE TABLE destination_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=10, SHARED REGEXP '^force$')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO source_04839
SELECT
    number,
    toJSONString(map('force', number, 'keep', number))
FROM numbers(8)
SETTINGS max_threads=1, max_block_size=2;

SELECT
    'source',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    countIf(has(JSONSharedDataPaths(j), 'keep'))
FROM source_04839;

-- Filtering clones the JSON column and squashing appends several filtered chunks. Both operations
-- must retain the immutable shared-path policy.
INSERT INTO destination_04839
SELECT id, j
FROM source_04839
WHERE id % 2 = 0
SETTINGS
    max_threads=1,
    max_insert_threads=1,
    max_block_size=2,
    min_insert_block_size_rows=1000,
    min_insert_block_size_bytes=1000000000;

SELECT
    'filtered and squashed',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    countIf(has(JSONSharedDataPaths(j), 'keep'))
FROM destination_04839;

DROP TABLE destination_04839;
DROP TABLE source_04839;
