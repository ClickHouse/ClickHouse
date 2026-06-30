-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 1;

SELECT throwIf(NOT isNull(arrayElement(CAST(NULL AS Nullable(Array(UInt8))), 1)))
FORMAT Null;

SELECT throwIf(toTypeName(arrayElement(CAST(NULL AS Nullable(Array(UInt8))), 1)) != 'Nullable(UInt8)')
FORMAT Null;

SELECT throwIf(groupArray(isNull(arrayElement(a, 1))) != [1, 0, 0])
FROM
(
    SELECT arrayJoin([
        CAST(NULL AS Nullable(Array(UInt8))),
        CAST([] AS Nullable(Array(UInt8))),
        CAST([1] AS Nullable(Array(UInt8)))
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayElement(a, 1), 99)) != [99, 0, 1])
FROM
(
    SELECT arrayJoin([
        CAST(NULL AS Nullable(Array(UInt8))),
        CAST([] AS Nullable(Array(UInt8))),
        CAST([1] AS Nullable(Array(UInt8)))
    ]) AS a
)
FORMAT Null;

SELECT 'ok';
