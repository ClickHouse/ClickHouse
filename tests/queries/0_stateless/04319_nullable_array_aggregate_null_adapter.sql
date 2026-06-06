-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 1;

SELECT throwIf(NOT isNull(any(arr)))
FROM
(
    SELECT CAST(NULL AS Nullable(Array(UInt8))) AS arr
    FROM numbers(3)
)
FORMAT Null;

SELECT throwIf(NOT isNull(anyIf(arr, 1)))
FROM
(
    SELECT CAST(NULL AS Nullable(Array(UInt8))) AS arr
    FROM numbers(3)
)
FORMAT Null;

SELECT throwIf(isNull(any(arr)) OR any(arr) != [1])
FROM
(
    SELECT arrayJoin([
        CAST(NULL AS Nullable(Array(UInt8))),
        CAST([1] AS Nullable(Array(UInt8)))
    ]) AS arr
)
FORMAT Null;

SELECT throwIf(isNull(anyIf(arr, cond)) OR anyIf(arr, cond) != [2])
FROM
(
    SELECT arrayJoin([
        (CAST(NULL AS Nullable(Array(UInt8))), 1),
        (CAST([1] AS Nullable(Array(UInt8))), 0),
        (CAST([2] AS Nullable(Array(UInt8))), 1)
    ]) AS t, t.1 AS arr, t.2 AS cond
)
FORMAT Null;
