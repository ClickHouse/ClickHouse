-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS inferred_nested_04839;

-- DataTypeObject::getTypeOfNestedObjects used to manufacture a single bare, pathless JSON type,
-- reused by ObjectJSONNode for every dynamically-inferred nested object regardless of where it
-- was found. A directly-nested object (e.g. {"outer": {...}}) doesn't exercise this: it gets
-- flattened into top-level paths during the initial object walk (traverseAndInsert), evaluated
-- against the root matcher directly. An array of objects with inconsistent shapes does exercise
-- it: `arr` itself becomes one Dynamic value of type Array(JSON(...)), and each element's own
-- type is built via the nested-object path -- so `forced` inside an element must still be
-- evaluated as "arr.forced" against the rule, not bare "forced".
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
