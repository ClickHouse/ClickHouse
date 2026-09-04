-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS alter_04839;

CREATE TABLE alter_04839
(
    id UInt64,
    marker UInt64,
    j JSON(max_dynamic_paths=1, SHARED REGEXP '^force$')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part=0,
    min_bytes_for_wide_part=0,
    max_bytes_to_merge_at_max_space_in_pool=1,
    vertical_merge_algorithm_min_rows_to_activate=1000000,
    vertical_merge_algorithm_min_columns_to_activate=1000000;

SYSTEM STOP MERGES alter_04839;

INSERT INTO alter_04839
SELECT
    number,
    number,
    if(
        number = 0,
        toJSONString(map('force', number, 'keep', number)),
        toJSONString(map('force', number)))
FROM numbers(4)
SETTINGS max_threads=1;

INSERT INTO alter_04839
SELECT
    number + 4,
    number + 4,
    if(
        number = 0,
        toJSONString(map('force', number + 4, 'keep', number + 4)),
        toJSONString(map('force', number + 4)))
FROM numbers(4)
SETTINGS max_threads=1;

SELECT
    'before alter',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    countIf(has(JSONSharedDataPaths(j), 'keep'))
FROM alter_04839;

-- Removing only the placement policy is metadata-only. By default, later merges preserve paths
-- that were already written to shared data instead of silently promoting them.
ALTER TABLE alter_04839 MODIFY COLUMN j JSON(max_dynamic_paths=1);

SELECT
    count() AS active_parts,
    (SELECT count() FROM system.mutations
     WHERE database=currentDatabase() AND table='alter_04839') AS mutations
FROM system.parts
WHERE database=currentDatabase() AND table='alter_04839' AND active;

-- DETACH/ATTACH reconstructs both the table metadata and each part's column metadata from disk.
-- The current table type has no rule, while both source parts retain it as placement provenance.
DETACH TABLE alter_04839;
ATTACH TABLE alter_04839;
SYSTEM STOP MERGES alter_04839;

SELECT
    'after metadata reload provenance',
    (SELECT position(c.type, 'SHARED REGEXP') = 0
     FROM system.columns AS c
     WHERE c.database=currentDatabase() AND c.table='alter_04839' AND c.name='j'),
    count(),
    countIf(position(p.type, 'SHARED REGEXP') > 0)
FROM system.parts_columns AS p
WHERE p.database=currentDatabase() AND p.table='alter_04839' AND p.column='j' AND p.active;

SELECT
    'after metadata alter',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    countIf(has(JSONSharedDataPaths(j), 'keep'))
FROM alter_04839;

SYSTEM START MERGES alter_04839;
OPTIMIZE TABLE alter_04839 FINAL;

SELECT
    'after preserving merge provenance',
    count(),
    countIf(position(p.type, 'SHARED REGEXP') > 0)
FROM system.parts_columns AS p
WHERE p.database=currentDatabase() AND p.table='alter_04839' AND p.column='j' AND p.active;

SELECT
    'after preserving merge',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    countIf(has(JSONSharedDataPaths(j), 'keep'))
FROM alter_04839;

-- A wide-part mutation can hard-link an untouched JSON column. Its old per-part type must be
-- retained alongside those files.
ALTER TABLE alter_04839
    UPDATE marker = marker + 1 WHERE id=0
    SETTINGS mutations_sync=2;

DETACH TABLE alter_04839;
ATTACH TABLE alter_04839;

SELECT
    'after unrelated mutation reload provenance',
    count(),
    countIf(position(p.type, 'SHARED REGEXP') > 0)
FROM system.parts_columns AS p
WHERE p.database=currentDatabase() AND p.table='alter_04839' AND p.column='j' AND p.active;

-- Rewriting the JSON column under the current rule-free table type must retain the source part's
-- policy by default; a second reload proves it was persisted, not just held in memory.
ALTER TABLE alter_04839
    UPDATE j = '{"force":100,"keep":100}' WHERE id=0
    SETTINGS mutations_sync=2;

DETACH TABLE alter_04839;
ATTACH TABLE alter_04839;

SELECT
    'after mutation reload provenance',
    count(),
    countIf(position(p.type, 'SHARED REGEXP') > 0)
FROM system.parts_columns AS p
WHERE p.database=currentDatabase() AND p.table='alter_04839' AND p.column='j' AND p.active;

SELECT
    'after preserving mutation',
    countIf(has(JSONDynamicPaths(j), 'force')),
    countIf(has(JSONSharedDataPaths(j), 'force')),
    countIf(has(JSONDynamicPaths(j), 'keep')),
    countIf(has(JSONSharedDataPaths(j), 'keep'))
FROM alter_04839;

-- Column renames must carry the per-part policy to the new physical column name.
ALTER TABLE alter_04839 RENAME COLUMN j TO payload SETTINGS mutations_sync=2;
DETACH TABLE alter_04839;
ATTACH TABLE alter_04839;
SYSTEM STOP MERGES alter_04839;

SELECT
    'after rename reload provenance',
    count(),
    countIf(position(p.type, 'SHARED REGEXP') > 0)
FROM system.parts_columns AS p
WHERE p.database=currentDatabase() AND p.table='alter_04839' AND p.column='payload' AND p.active;

SELECT
    'after rename',
    countIf(has(JSONDynamicPaths(payload), 'force')),
    countIf(has(JSONSharedDataPaths(payload), 'force')),
    countIf(has(JSONDynamicPaths(payload), 'keep')),
    countIf(has(JSONSharedDataPaths(payload), 'keep'))
FROM alter_04839;

-- Re-promotion is an explicit table-level opt-in. Force a rewrite of the single final part so the
-- higher-frequency `force` path can take the one dynamic-path slot.
ALTER TABLE alter_04839 MODIFY SETTING allow_json_shared_data_paths_repromotion=1;
DETACH TABLE alter_04839;
ATTACH TABLE alter_04839;
SYSTEM START MERGES alter_04839;
OPTIMIZE TABLE alter_04839 FINAL SETTINGS optimize_skip_merged_partitions=0;

SELECT
    'after opted-in rewrite provenance',
    count(),
    countIf(position(p.type, 'SHARED REGEXP') > 0)
FROM system.parts_columns AS p
WHERE p.database=currentDatabase() AND p.table='alter_04839' AND p.column='payload' AND p.active;

SELECT
    'after opted-in rewrite',
    countIf(has(JSONDynamicPaths(payload), 'force')),
    countIf(has(JSONSharedDataPaths(payload), 'force')),
    countIf(has(JSONDynamicPaths(payload), 'keep')),
    countIf(has(JSONSharedDataPaths(payload), 'keep'))
FROM alter_04839;

DROP TABLE alter_04839;

-- With re-promotion opted in, a mutation rewrite must also re-decide placement: paths that an
-- already-removed rule forced into shared data move back to dynamic paths, like OPTIMIZE ... FINAL.
DROP TABLE IF EXISTS mutation_repromotion_compact_04839;

CREATE TABLE mutation_repromotion_compact_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part=1000000,
    min_bytes_for_wide_part=1000000000,
    max_bytes_to_merge_at_max_space_in_pool=1;

INSERT INTO mutation_repromotion_compact_04839 VALUES (1, '{"tag_a":1,"keep":1}'), (2, '{"tag_b":2,"keep":2}');

SELECT
    'compact mutation repromotion before',
    id,
    arraySort(JSONDynamicPaths(j)),
    arraySort(JSONSharedDataPaths(j))
FROM mutation_repromotion_compact_04839
ORDER BY id;

ALTER TABLE mutation_repromotion_compact_04839 MODIFY COLUMN j JSON(max_dynamic_paths=5);
ALTER TABLE mutation_repromotion_compact_04839 MODIFY SETTING allow_json_shared_data_paths_repromotion=1;
ALTER TABLE mutation_repromotion_compact_04839 UPDATE j = j WHERE 1 SETTINGS mutations_sync=2;

SELECT
    'compact mutation repromotion provenance',
    count(),
    countIf(position(type, 'SHARED REGEXP') > 0),
    any(part_type)
FROM system.parts_columns
WHERE database=currentDatabase() AND table='mutation_repromotion_compact_04839' AND column='j' AND active;

SELECT
    'compact mutation repromotion',
    id,
    arraySort(JSONDynamicPaths(j)),
    arraySort(JSONSharedDataPaths(j))
FROM mutation_repromotion_compact_04839
ORDER BY id;

DROP TABLE mutation_repromotion_compact_04839;

-- The same re-promotion through the wide-part mutation path, which rewrites only the updated column.
DROP TABLE IF EXISTS mutation_repromotion_wide_04839;

CREATE TABLE mutation_repromotion_wide_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part=0,
    min_bytes_for_wide_part=0,
    max_bytes_to_merge_at_max_space_in_pool=1;

INSERT INTO mutation_repromotion_wide_04839 VALUES (1, '{"tag_a":1,"keep":1}'), (2, '{"tag_b":2,"keep":2}');

SELECT
    'wide mutation repromotion before',
    id,
    arraySort(JSONDynamicPaths(j)),
    arraySort(JSONSharedDataPaths(j))
FROM mutation_repromotion_wide_04839
ORDER BY id;

ALTER TABLE mutation_repromotion_wide_04839 MODIFY COLUMN j JSON(max_dynamic_paths=5);
ALTER TABLE mutation_repromotion_wide_04839 MODIFY SETTING allow_json_shared_data_paths_repromotion=1;
ALTER TABLE mutation_repromotion_wide_04839 UPDATE j = j WHERE 1 SETTINGS mutations_sync=2;

SELECT
    'wide mutation repromotion provenance',
    count(),
    countIf(position(type, 'SHARED REGEXP') > 0),
    any(part_type)
FROM system.parts_columns
WHERE database=currentDatabase() AND table='mutation_repromotion_wide_04839' AND column='j' AND active;

SELECT
    'wide mutation repromotion',
    id,
    arraySort(JSONDynamicPaths(j)),
    arraySort(JSONSharedDataPaths(j))
FROM mutation_repromotion_wide_04839
ORDER BY id;

DROP TABLE mutation_repromotion_wide_04839;
