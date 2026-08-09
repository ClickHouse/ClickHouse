-- `arrayCount` returns `UInt64` by default, and `UInt32` (the type it returned before version 26.8)
-- under the `array_count_legacy_uint32_result` compatibility setting.

SELECT arrayCount(x -> (x % 2), [1, 2, 3]) AS count, toTypeName(count);
SELECT arrayCount(x -> (x % 2), [1, 2, 3]) AS count, toTypeName(count) SETTINGS array_count_legacy_uint32_result = 1;
SELECT arrayCount([1, 0, 2]) AS count, toTypeName(count);
SELECT arrayCount([1, 0, 2]) AS count, toTypeName(count) SETTINGS array_count_legacy_uint32_result = 1;

-- The constant-filter path (the predicate does not depend on the array elements).
SELECT arrayCount(x -> 1, [1, 2, 3]) AS count, toTypeName(count);
SELECT arrayCount(x -> 1, [1, 2, 3]) AS count, toTypeName(count) SETTINGS array_count_legacy_uint32_result = 1;
SELECT arrayCount(x -> 0, [1, 2, 3]) AS count, toTypeName(count);
SELECT arrayCount(x -> 0, [1, 2, 3]) AS count, toTypeName(count) SETTINGS array_count_legacy_uint32_result = 1;

-- The `compatibility` setting restores the legacy type.
SELECT arrayCount(x -> (x % 2), materialize([1, 2, 3])) AS count, toTypeName(count) SETTINGS compatibility = '26.7';

-- The rewrite of `length(arrayFilter(...))` requires the types to match, so it does not fire in legacy mode.
SELECT length(arrayFilter(x -> (x % 2), [1, 2, 3])) AS count, toTypeName(count)
SETTINGS array_count_legacy_uint32_result = 1, optimize_rewrite_array_filter_length_to_array_count = 1;

-- And the query tree proves it: `arrayFilter` stays and no `arrayCount` appears in legacy mode.
SELECT
    countIf(explain LIKE '%function_name: arrayCount%') AS array_count,
    countIf(explain LIKE '%function_name: arrayFilter%') AS array_filter
FROM
(
    EXPLAIN QUERY TREE SELECT length(arrayFilter(x -> (x % 2), materialize([1, 2, 3])))
    SETTINGS array_count_legacy_uint32_result = 1, optimize_rewrite_array_filter_length_to_array_count = 1
)
SETTINGS enable_analyzer = 1;
