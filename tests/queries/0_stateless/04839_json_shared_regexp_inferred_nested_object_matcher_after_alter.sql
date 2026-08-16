-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS inferred_nested_matcher_after_alter_04839;

-- setSharedDataPathMatcherRecursively's Object branch only walked typed_paths, missing values
-- dynamically inferred as nested JSON (inconsistent-shape arrays); a policy-only ALTER left them on the stale matcher/prefix.
CREATE TABLE inferred_nested_matcher_after_alter_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=5, SHARED REGEXP '^arr[.]forced$')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO inferred_nested_matcher_after_alter_04839 VALUES
    (1, '{"arr":[{"forced":1,"keep":3},{"forced":4,"other":6}]}');

SELECT 'before ALTER', JSONAllPathsWithTypes(j.arr[1]), JSONAllPathsWithTypes(j.arr[2])
FROM inferred_nested_matcher_after_alter_04839;

-- Metadata-only for JSON nested inside Array (see the doc comment on
-- DataTypeObject::getTypeOfNestedObjects): does not rewrite the existing part.
ALTER TABLE inferred_nested_matcher_after_alter_04839
    MODIFY COLUMN j JSON(max_dynamic_paths=5, SHARED REGEXP '^arr[.]newforced$');

SELECT 'after ALTER', JSONAllPathsWithTypes(j.arr[1]), JSONAllPathsWithTypes(j.arr[2])
FROM inferred_nested_matcher_after_alter_04839;

DETACH TABLE inferred_nested_matcher_after_alter_04839;
ATTACH TABLE inferred_nested_matcher_after_alter_04839;

SELECT 'after reload', JSONAllPathsWithTypes(j.arr[1]), JSONAllPathsWithTypes(j.arr[2])
FROM inferred_nested_matcher_after_alter_04839;

DROP TABLE inferred_nested_matcher_after_alter_04839;
