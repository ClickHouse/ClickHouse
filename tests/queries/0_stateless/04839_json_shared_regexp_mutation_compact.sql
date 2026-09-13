-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS compact_mutation_04839;

CREATE TABLE compact_mutation_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=1, SHARED REGEXP '^force$')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part=1000000,
    min_bytes_for_wide_part=1000000000;

INSERT INTO compact_mutation_04839
SELECT
    number,
    if(
        number IN (0, 4),
        toJSONString(map('force', number, 'keep', number)),
        toJSONString(map('force', number)))
FROM numbers(8)
SETTINGS max_threads=1;

ALTER TABLE compact_mutation_04839 MODIFY COLUMN j JSON(max_dynamic_paths=1);

-- Compact parts rewrite all columns for a mutation. The rewritten part must preserve the source
-- policy by default, including after the in-memory metadata is reconstructed from disk.
ALTER TABLE compact_mutation_04839
    UPDATE j = '{"force":200,"keep":200}' WHERE id=0
    SETTINGS mutations_sync=2;

DETACH TABLE compact_mutation_04839;
ATTACH TABLE compact_mutation_04839;

SELECT
    'compact mutation provenance',
    count(),
    countIf(position(type, 'SHARED REGEXP') > 0),
    any(part_type)
FROM system.parts_columns
WHERE database=currentDatabase() AND table='compact_mutation_04839' AND column='j' AND active;

SELECT
    'compact mutation placement',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    countIf(has(JSONSharedDataPaths(j), 'keep'))
FROM compact_mutation_04839;

DROP TABLE compact_mutation_04839;
