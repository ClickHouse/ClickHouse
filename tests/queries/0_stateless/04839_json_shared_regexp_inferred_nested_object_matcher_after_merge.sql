-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS inferred_nested_matcher_after_merge_04839;

-- Companion to the after_alter test: covers the merge path, where the matcher-propagation call
-- runs before chooseDynamicStructureForMerge rebuilds nested variants and decides placement, so patching after is too late.
CREATE TABLE inferred_nested_matcher_after_merge_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=5, SHARED REGEXP '^arr[.]forced$')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_json_shared_data_paths_repromotion = 1;

SYSTEM STOP MERGES inferred_nested_matcher_after_merge_04839;

INSERT INTO inferred_nested_matcher_after_merge_04839 VALUES
    (1, '{"arr":[{"forced":1,"keep":3},{"forced":4,"other":6}]}');
INSERT INTO inferred_nested_matcher_after_merge_04839 VALUES
    (2, '{"arr":[{"forced":7,"keep":8},{"forced":9,"other":10}]}');

SELECT 'parts before merge', count() FROM system.parts
WHERE database = currentDatabase() AND table = 'inferred_nested_matcher_after_merge_04839' AND active;

SELECT 'before merge', id, JSONDynamicPaths(j.arr[1]), JSONSharedDataPaths(j.arr[1])
FROM inferred_nested_matcher_after_merge_04839 ORDER BY id;

ALTER TABLE inferred_nested_matcher_after_merge_04839
    MODIFY COLUMN j JSON(max_dynamic_paths=5, SHARED REGEXP '^arr[.]neverused$');

SYSTEM START MERGES inferred_nested_matcher_after_merge_04839;
OPTIMIZE TABLE inferred_nested_matcher_after_merge_04839 FINAL;

SELECT 'parts after merge', count() FROM system.parts
WHERE database = currentDatabase() AND table = 'inferred_nested_matcher_after_merge_04839' AND active;

SELECT 'after merge', id, JSONDynamicPaths(j.arr[1]), JSONSharedDataPaths(j.arr[1])
FROM inferred_nested_matcher_after_merge_04839 ORDER BY id;

DROP TABLE inferred_nested_matcher_after_merge_04839;
