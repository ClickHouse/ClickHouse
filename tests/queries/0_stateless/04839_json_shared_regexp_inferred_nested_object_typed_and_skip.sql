-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS inferred_nested_typed_skip_04839;

-- getTypeOfNestedObjects used to carry SHARED REGEXP into inferred nested objects but drop the
-- parent's typed/SKIP paths under the same prefix, inverting their documented precedence.
CREATE TABLE inferred_nested_typed_skip_04839
(
    id UInt64,
    j JSON(`arr.forced` UInt64, SKIP arr.skip, SHARED REGEXP '^arr[.]forced$')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO inferred_nested_typed_skip_04839 VALUES
    (1, '{"arr":[{"forced":1,"skip":2,"keep":3},{"forced":4,"skip":5,"other":6}]}');

-- The regression: without projecting the typed path and SKIP, `forced` would show as a dynamic type
-- (not UInt64) and `skip` would leak into each element; j.arr[1].forced alone is NULL (pre-existing gap), hence JSONAllPathsWithTypes.
SELECT
    'typed and skipped paths inside inferred nested elements',
    JSONAllPathsWithTypes(j.arr[1]),
    JSONAllPathsWithTypes(j.arr[2]),
    j.arr[1].forced,
    j.arr[2].forced
FROM inferred_nested_typed_skip_04839;

-- Must survive a metadata reload, not just exist in the freshly-parsed in-memory column.
DETACH TABLE inferred_nested_typed_skip_04839;
ATTACH TABLE inferred_nested_typed_skip_04839;

SELECT
    'typed and skipped paths inside inferred nested elements after reload',
    JSONAllPathsWithTypes(j.arr[1]),
    JSONAllPathsWithTypes(j.arr[2]),
    j.arr[1].forced,
    j.arr[2].forced
FROM inferred_nested_typed_skip_04839;

DROP TABLE inferred_nested_typed_skip_04839;
