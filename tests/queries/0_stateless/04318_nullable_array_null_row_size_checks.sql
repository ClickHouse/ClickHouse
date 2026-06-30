-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 1;

SELECT throwIf(NOT isNull(arrayMap(
    (x, y) -> x + y,
    CAST(NULL AS Nullable(Array(Int32))),
    CAST([1] AS Nullable(Array(Int32))))))
FORMAT Null;

SELECT throwIf(NOT isNull(arrayFold(
    (acc, x, y) -> acc + x + y,
    CAST(NULL AS Nullable(Array(Int32))),
    CAST([1] AS Nullable(Array(Int32))),
    0::Int64)))
FORMAT Null;

SELECT throwIf(NOT isNull(arrayReduce(
    'sumIf',
    CAST(NULL AS Nullable(Array(Int32))),
    CAST([1] AS Nullable(Array(UInt8))))))
FORMAT Null;

SELECT throwIf(NOT isNull(arrayReduceInRanges(
    'sumIf',
    CAST([(1, 1)] AS Nullable(Array(Tuple(Int64, UInt64)))),
    CAST(NULL AS Nullable(Array(Int32))),
    CAST([1] AS Nullable(Array(UInt8))))))
FORMAT Null;

SELECT throwIf(NOT isNull(arrayZip(
    CAST(NULL AS Nullable(Array(Int32))),
    CAST([1] AS Nullable(Array(Int32))))))
FORMAT Null;

SELECT throwIf(NOT isNull(
    materialize(CAST(NULL AS Nullable(Array(Int32))))
    + materialize(CAST([1] AS Nullable(Array(Int32))))))
FROM numbers(1)
FORMAT Null;

SELECT throwIf(NOT isNull(arrayEnumerateDense(
    CAST(NULL AS Nullable(Array(Int32))),
    CAST([1] AS Nullable(Array(Int32))))))
FORMAT Null;

SELECT throwIf(NOT isNull(arrayEnumerateUniq(
    CAST(NULL AS Nullable(Array(Int32))),
    CAST([1] AS Nullable(Array(Int32))))))
FORMAT Null;

SELECT throwIf(NOT isNull(arrayEnumerateUniqRanked(
    CAST(NULL AS Nullable(Array(Int32))),
    CAST([1] AS Nullable(Array(Int32))))))
FORMAT Null;
