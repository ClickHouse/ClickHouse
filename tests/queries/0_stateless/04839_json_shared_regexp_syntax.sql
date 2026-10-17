-- Tags: no-fasttest, no-random-settings

SET enable_json_type = 1;

-- SHARED remains a valid typed-path name unless it is followed by REGEXP.
SELECT toTypeName(CAST('{}', 'JSON(shared UInt32)'));
SELECT toTypeName(CAST('{}', 'JSON(shared.foo UInt32)'));

-- Rules have a deterministic canonical order.
SELECT toTypeName('{}'::JSON(SHARED REGEXP '^z', SHARED REGEXP '^a'));

-- SHARED REGEXP matching is partial and is independent of the SKIP REGEXP setting.
SELECT
    'partial ambient=0',
    arraySort(JSONDynamicPaths(j)),
    arraySort(JSONSharedDataPaths(j))
FROM format(
    JSONEachRow,
    'j JSON(max_dynamic_paths=10, SHARED REGEXP \'foo\')',
    '{"j":{"foo":1,"foobar":2,"keep":3}}')
SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 0;

SELECT
    'partial ambient=1',
    arraySort(JSONDynamicPaths(j)),
    arraySort(JSONSharedDataPaths(j))
FROM format(
    JSONEachRow,
    'j JSON(max_dynamic_paths=10, SHARED REGEXP \'foo\')',
    '{"j":{"foo":1,"foobar":2,"keep":3}}')
SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 1;

-- Paths flattened through nested objects are matched, paths of objects inside arrays are not.
SELECT
    'nested objects and arrays',
    arraySort(JSONDynamicPaths(j)),
    arraySort(JSONSharedDataPaths(j)),
    arrayMap(x -> JSONDynamicPaths(x), j.arr.:`Array(JSON)`)
FROM format(
    JSONEachRow,
    'j JSON(max_dynamic_paths=10, SHARED REGEXP \'tag_\')',
    '{"j":{"a":{"b":{"tag_x":1}},"arr":[{"tag_x":2}],"keep":3}}');

-- Typed paths and SKIP paths take precedence over SHARED REGEXP.
SELECT
    j.foo,
    arraySort(JSONDynamicPaths(j)),
    arraySort(JSONSharedDataPaths(j))
FROM format(
    JSONEachRow,
    'j JSON(max_dynamic_paths=10, foo UInt64, SHARED REGEXP \'foo\')',
    '{"j":{"foo":7}}');

SELECT empty(JSONAllPaths(j))
FROM format(
    JSONEachRow,
    'j JSON(max_dynamic_paths=10, SKIP foo, SHARED REGEXP \'foo\')',
    '{"j":{"foo":7}}');

-- A path matching both SKIP REGEXP and SHARED REGEXP is discarded, not stored in shared data.
SELECT empty(JSONAllPaths(j))
FROM format(
    JSONEachRow,
    'j JSON(max_dynamic_paths=10, SKIP REGEXP \'foo\', SHARED REGEXP \'foo\')',
    '{"j":{"foo":7}}');

SELECT toTypeName('{}'::JSON(SHARED REGEXP '[')); -- { serverError CANNOT_COMPILE_REGEXP }

-- generateRandom follows the rule too.
SELECT 'generateRandom', countIf(notEmpty(JSONDynamicPaths(j))), countIf(notEmpty(JSONSharedDataPaths(j))) > 0
FROM (SELECT j FROM generateRandom('j JSON(max_dynamic_paths=10, SHARED REGEXP \'.*\')', 42, 10, 3) LIMIT 100);
