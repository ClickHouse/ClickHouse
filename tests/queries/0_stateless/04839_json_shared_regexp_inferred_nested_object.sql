-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS inferred_nested_04839;

-- DataTypeObject::getTypeOfNestedObjects used to manufacture a bare, pathless JSON type for every
-- inferred nested object; array elements must still resolve as "arr.forced", not bare "forced".
CREATE TABLE inferred_nested_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=10, SHARED REGEXP '^arr[.]forced$')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO inferred_nested_04839 VALUES (1, '{"arr":[{"forced":1,"keep":2},{"forced":3,"other":4}]}');

-- The regression: this must show the rule and the "arr." prefix. Without propagating them into
-- the nested-object type, this comes back as a bare JSON(...) with neither.
SELECT
    'inferred nested element type',
    position(dynamicType(j.arr), 'SHARED REGEXP') > 0,
    position(dynamicType(j.arr), 'shared_regexp_path_prefix=\'arr.\'') > 0
FROM inferred_nested_04839;

-- Must survive a metadata reload, not just exist in the freshly-parsed in-memory column.
DETACH TABLE inferred_nested_04839;
ATTACH TABLE inferred_nested_04839;

SELECT
    'inferred nested element type after reload',
    position(dynamicType(j.arr), 'SHARED REGEXP') > 0,
    position(dynamicType(j.arr), 'shared_regexp_path_prefix=\'arr.\'') > 0
FROM inferred_nested_04839;

DROP TABLE inferred_nested_04839;
