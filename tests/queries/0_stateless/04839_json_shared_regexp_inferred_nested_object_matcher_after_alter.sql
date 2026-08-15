-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS inferred_nested_matcher_after_alter_04839;

-- setSharedDataPathMatcherRecursively (ColumnObject.cpp) pushes the current SHARED REGEXP
-- matcher/prefix onto an in-memory column during reads/writes/merges/gathering, but its Object
-- branch only walked typed_paths -- a value dynamically inferred as its own nested JSON object
-- (see ObjectJSONNode::getDynamicNodeForPath in JSONExtractTree.cpp) lives under a *dynamic* path
-- instead, wrapped in Array for an inconsistent-shape array like `arr` below, so the update never
-- reached it and a policy-only ALTER left it on its original matcher/prefix. This is a
-- reachability/smoke regression test: it exercises the exact shape (inconsistent-shape array
-- forcing arr's elements to infer as nested objects, then a policy-only ALTER, then a read) that
-- the fix's code path depends on, and confirms it continues to parse and read back correctly
-- across a metadata reload. The internal matcher-propagation fix itself (does the new prefix
-- actually reach the nested ColumnObject) was verified separately by direct inspection of
-- setSharedDataPathMatcherRecursively's behavior, since the matcher is in-memory state with no
-- direct SQL-level introspection.
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
