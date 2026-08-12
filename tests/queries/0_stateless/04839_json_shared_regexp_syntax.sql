-- Tags: no-fasttest, no-random-settings

SET enable_json_type = 1;

-- SHARED remains a valid typed-path name unless it is followed by REGEXP.
SELECT toTypeName(CAST('{}', 'JSON(shared UInt32)'));
SELECT toTypeName(CAST('{}', 'JSON(shared.foo UInt32)'));

-- Rules have a deterministic canonical order and the default mode is not printed.
SELECT toTypeName('{}'::JSON(SHARED REGEXP '^z', SHARED REGEXP '^a'));

-- Full-match mode is a persisted JSON type parameter, not an ambient format setting.
WITH toTypeName('{}'::JSON(shared_regexp_use_partial_match=0, SHARED REGEXP 'foo')) AS type
SELECT
    position(type, 'shared_regexp_use_partial_match=0') > 0,
    position(type, 'SHARED REGEXP FULL') = 0;

-- Default SHARED REGEXP matching is partial and is independent of the SKIP REGEXP setting.
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

SELECT
    'full ambient=0',
    arraySort(JSONDynamicPaths(j)),
    arraySort(JSONSharedDataPaths(j))
FROM format(
    JSONEachRow,
    'j JSON(max_dynamic_paths=10, shared_regexp_use_partial_match=0, SHARED REGEXP \'foo\')',
    '{"j":{"foo":1,"foobar":2,"keep":3}}')
SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 0;

SELECT
    'full ambient=1',
    arraySort(JSONDynamicPaths(j)),
    arraySort(JSONSharedDataPaths(j))
FROM format(
    JSONEachRow,
    'j JSON(max_dynamic_paths=10, shared_regexp_use_partial_match=0, SHARED REGEXP \'foo\')',
    '{"j":{"foo":1,"foobar":2,"keep":3}}')
SETTINGS type_json_use_partial_match_to_skip_paths_by_regexp = 1;

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

-- A sub-object exposes root paths without their prefix. Matching must still use the original,
-- root-relative path, including when several sub-object prefixes are accumulated.
SELECT
    arraySort(JSONDynamicPaths(j)),
    arraySort(JSONSharedDataPaths(j)),
    arraySort(JSONDynamicPaths(j.^outer)),
    arraySort(JSONSharedDataPaths(j.^outer)),
    arraySort(JSONDynamicPaths(j.^outer.inner)),
    arraySort(JSONSharedDataPaths(j.^outer.inner))
FROM format(
    JSONEachRow,
    'j JSON(max_dynamic_paths=10, SHARED REGEXP \'^outer[.]inner[.]forced$\')',
    '{"j":{"outer":{"inner":{"forced":1,"keep":2},"forced":3},"forced":4}}');

-- The derived sub-object type carries its accumulated root prefix in its canonical name. That
-- name must itself be accepted as a type declaration so it can cross text-based type boundaries.
WITH toTypeName(j.^outer) AS sub_type
SELECT
    position(sub_type, 'shared_regexp_path_prefix=\'outer.\'') > 0,
    toTypeName(CAST('{}', sub_type)) = sub_type
FROM format(
    JSONEachRow,
    'j JSON(max_dynamic_paths=10, SHARED REGEXP \'^outer[.]forced$\')',
    '{"j":{"outer":{"forced":1,"keep":2}}}');

SELECT toTypeName('{}'::JSON(SHARED REGEXP '')); -- { serverError BAD_ARGUMENTS }
SELECT toTypeName('{}'::JSON(SHARED REGEXP '[')); -- { serverError CANNOT_COMPILE_REGEXP }
