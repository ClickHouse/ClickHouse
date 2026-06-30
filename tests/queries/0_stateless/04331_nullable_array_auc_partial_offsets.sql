SET allow_experimental_nullable_array_type = 1;

SELECT throwIf(NOT isNull(arrayAUCPR(
    [0.1],
    [1],
    CAST(NULL AS Nullable(Array(UInt64))))))
FORMAT Null;

SELECT throwIf(NOT isNull(arrayROCAUC(
    [0.1],
    [1],
    true,
    CAST(NULL AS Nullable(Array(UInt64))))))
FORMAT Null;

SELECT throwIf(NOT isNull(arrayAUCPR(
    CAST([0.1, 0.2] AS Nullable(Array(Float64))),
    CAST([1] AS Nullable(Array(UInt8))),
    CAST(NULL AS Nullable(Array(UInt64))))))
FORMAT Null;

SELECT throwIf(NOT isNull(arrayROCAUC(
    CAST([0.1, 0.2] AS Nullable(Array(Float64))),
    CAST([1] AS Nullable(Array(UInt8))),
    true,
    CAST(NULL AS Nullable(Array(UInt64))))))
FORMAT Null;
