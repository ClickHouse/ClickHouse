-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS inferred_nested_skip_regexp_04839;

-- getTypeOfNestedObjects used to hand inferred nested objects an empty path_regexps_to_skip, so
-- SKIP REGEXP paths leaked into inferred array elements instead of being discarded.
CREATE TABLE inferred_nested_skip_regexp_04839
(
    id UInt64,
    j JSON(SKIP REGEXP '^arr[.]skip$', SHARED REGEXP '^arr[.]forced$')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO inferred_nested_skip_regexp_04839 VALUES
    (1, '{"arr":[{"forced":1,"skip":2,"keep":3},{"forced":4,"skip":5,"other":6}]}');

-- The regression: without root-relative reconstruction, "skip" would still appear inside each
-- inferred element instead of being discarded entirely.
SELECT
    'skipped regexp paths inside inferred nested elements',
    JSONAllPathsWithTypes(j.arr[1]),
    JSONAllPathsWithTypes(j.arr[2])
FROM inferred_nested_skip_regexp_04839;

-- Must survive a metadata reload, not just exist in the freshly-parsed in-memory column.
DETACH TABLE inferred_nested_skip_regexp_04839;
ATTACH TABLE inferred_nested_skip_regexp_04839;

SELECT
    'skipped regexp paths inside inferred nested elements after reload',
    JSONAllPathsWithTypes(j.arr[1]),
    JSONAllPathsWithTypes(j.arr[2])
FROM inferred_nested_skip_regexp_04839;

DROP TABLE inferred_nested_skip_regexp_04839;
