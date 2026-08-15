-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS inferred_nested_matcher_after_merge_04839;

-- Companion to 04839_json_shared_regexp_inferred_nested_object_matcher_after_alter, which only
-- covers the metadata-only ALTER path and explicitly does not prove the merge/mutation-rewrite
-- path. This test covers that path: MergedData::initialize/ColumnGathererStream::initialize call
-- setSharedDataPathMatcherRecursively on the destination column, but *before*
-- chooseDynamicStructureForMerge runs -- and that call (via ColumnDynamic::setVariantType) rebuilds
-- each dynamic path's variant structure from the union of source types, which can recreate a JSON
-- variant (direct or wrapped, e.g. an inconsistent-shape array's elements) still carrying whatever
-- SHARED REGEXP policy its *source part* was written under. Worse, ColumnObject::choosePathPlacementForMerge's
-- shouldForceSharedData filtering -- which decides whether a path competes for promotion at all --
-- runs *during* that same rebuild, deep inside the recursive chooseDynamicStructureForMerge descent,
-- so patching the matcher only after the top-level call returns is too late to affect it: the
-- (wrong) placement decision has already been made by then.
--
-- `forced` appears in every array element (so it would win the single available nested slot on
-- stats alone) but is force-excluded by the original SHARED REGEXP rule; `keep`/`other` are weaker,
-- unforced competitors. After the rule is retired and a real merge runs with
-- allow_json_shared_data_paths_repromotion enabled, `forced` must be able to win its slot back --
-- unlike the ALTER-only companion test, this is checked directly via JSONDynamicPaths/
-- JSONSharedDataPaths on the nested object, which is exactly what the retired rule controls.
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
