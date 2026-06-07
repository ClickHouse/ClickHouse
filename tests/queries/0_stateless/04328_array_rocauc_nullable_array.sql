-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 1;

SELECT throwIf(groupArray(isNull(arrayROCAUC(a, b))) != [1, 0])
FROM
(
    SELECT arrayJoin([
        (CAST(NULL AS Nullable(Array(Float64))), CAST([1] AS Nullable(Array(UInt8)))),
        (CAST([0.1] AS Nullable(Array(Float64))), CAST([1] AS Nullable(Array(UInt8))))
    ]) AS t, t.1 AS a, t.2 AS b
)
FORMAT Null;

SELECT throwIf(NOT isNull(arrayROCAUC(CAST(NULL AS Nullable(Array(Float64))), CAST([1] AS Nullable(Array(UInt8))))))
FORMAT Null;

SELECT throwIf(floor(arrayROCAUC(CAST([0.1, 0.4, 0.35, 0.8] AS Nullable(Array(Float64))), CAST([0, 0, 1, 1] AS Nullable(Array(UInt8)))), 10) != 0.75)
FORMAT Null;
