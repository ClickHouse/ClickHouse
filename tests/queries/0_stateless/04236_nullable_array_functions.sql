-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 1;

SELECT throwIf(arrayPopBack(CAST([1, 2, 3], 'Nullable(Array(UInt8))')) != [1, 2])
FORMAT Null;

SELECT throwIf(arrayPopFront(CAST([1, 2, 3], 'Nullable(Array(UInt8))')) != [2, 3])
FORMAT Null;

SELECT throwIf(groupArray(x) != [[1], [1]])
FROM (SELECT arrayPopBack(CAST([1, 2], 'Nullable(Array(UInt8))')) AS x FROM numbers(2))
FORMAT Null;

SELECT throwIf(arrayPushBack(CAST([1, 2], 'Nullable(Array(UInt8))'), 3) != [1, 2, 3])
FORMAT Null;

SELECT throwIf(arrayPushFront(CAST([1, 2], 'Nullable(Array(UInt8))'), 0) != [0, 1, 2])
FORMAT Null;

SELECT throwIf(arraySlice(CAST([1, 2, 3, 4], 'Nullable(Array(UInt8))'), 2, 2) != [2, 3])
FORMAT Null;

SELECT throwIf(arraySlice(CAST([1, 2, 3], 'Nullable(Array(UInt8))'), -2) != [2, 3])
FORMAT Null;

SELECT throwIf(arrayResize(CAST([1, 2], 'Nullable(Array(UInt8))'), 4, 0) != [1, 2, 0, 0])
FORMAT Null;

SELECT throwIf(arrayResize(CAST([1, 2], 'Nullable(Array(UInt8))'), 1) != [1])
FORMAT Null;

SELECT throwIf(arrayRemove(CAST([1, 2, 2, 3], 'Nullable(Array(UInt8))'), 2) != [1, 3])
FORMAT Null;

SELECT throwIf(has(CAST([1, 2, 3], 'Nullable(Array(UInt8))'), 2) != 1)
FORMAT Null;

SELECT throwIf(has(CAST([1, 2, 3], 'Nullable(Array(UInt8))'), 9) != 0)
FORMAT Null;

SELECT throwIf(indexOf(CAST([1, 2, 3], 'Nullable(Array(UInt8))'), 3) != 3)
FORMAT Null;

SELECT throwIf(indexOf(CAST([1, 2, 3], 'Nullable(Array(UInt8))'), 9) != 0)
FORMAT Null;

SELECT throwIf(length(CAST([1, 2], 'Nullable(Array(UInt8))')) != 2)
FORMAT Null;

SELECT throwIf(empty(CAST([], 'Nullable(Array(UInt8))')) != 1)
FORMAT Null;

SELECT throwIf(empty(CAST([1], 'Nullable(Array(UInt8))')) != 0)
FORMAT Null;

SELECT throwIf(arrayElement(CAST([10, 20], 'Nullable(Array(UInt8))'), 2) != 20)
FORMAT Null;

SELECT throwIf(NOT isNull(arrayPopBack(CAST(NULL, 'Nullable(Array(UInt8))'))))
FORMAT Null;

SELECT throwIf(NOT isNull(arraySlice(CAST(NULL, 'Nullable(Array(UInt8))'), 1, 1)))
FORMAT Null;

SELECT throwIf(NOT isNull(length(CAST(NULL, 'Nullable(Array(UInt8))'))))
FORMAT Null;

SELECT throwIf(has(CAST(NULL, 'Nullable(Array(UInt8))'), 1) != 0)
FORMAT Null;

SELECT throwIf(indexOf(CAST(NULL, 'Nullable(Array(UInt8))'), 1) != 0)
FORMAT Null;

SELECT throwIf(groupArray(has(CAST(NULL, 'Nullable(Array(UInt8))'), number)) != [0, 0, 0])
FROM numbers(3)
FORMAT Null;

SELECT throwIf(arrayMap(x -> x + 1, CAST([1, NULL, 3], 'Nullable(Array(Nullable(UInt8)))')) != [2, NULL, 4])
FORMAT Null;

SELECT throwIf(has(CAST([1, NULL, 3], 'Nullable(Array(Nullable(UInt8)))'), NULL) != 1)
FORMAT Null;

SELECT throwIf(groupArray(isNull(arrayPopBack(a))) != [1, 0, 0])
FROM
(
    SELECT arrayJoin([
        CAST(NULL, 'Nullable(Array(Int32))'),
        CAST([], 'Nullable(Array(Int32))'),
        CAST([1, 2, 3], 'Nullable(Array(Int32))')
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayPopBack(a), [])) != [[], [], [1, 2]])
FROM
(
    SELECT arrayJoin([
        CAST(NULL, 'Nullable(Array(Int32))'),
        CAST([], 'Nullable(Array(Int32))'),
        CAST([1, 2, 3], 'Nullable(Array(Int32))')
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayPushBack(a, 99), [99])) != [[99], [99], [1, 2, 3, 99]])
FROM
(
    SELECT arrayJoin([
        CAST(NULL, 'Nullable(Array(Int32))'),
        CAST([], 'Nullable(Array(Int32))'),
        CAST([1, 2, 3], 'Nullable(Array(Int32))')
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arraySlice(a, 2, 2), [])) != [[], [], [2, 3]])
FROM
(
    SELECT arrayJoin([
        CAST(NULL, 'Nullable(Array(Int32))'),
        CAST([], 'Nullable(Array(Int32))'),
        CAST([1, 2, 3], 'Nullable(Array(Int32))')
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayResize(a, 2, -1), [])) != [[], [-1, -1], [1, 2]])
FROM
(
    SELECT arrayJoin([
        CAST(NULL, 'Nullable(Array(Int32))'),
        CAST([], 'Nullable(Array(Int32))'),
        CAST([1, 2, 3], 'Nullable(Array(Int32))')
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(has(a, 2), 0)) != [0, 0, 1])
FROM
(
    SELECT arrayJoin([
        CAST(NULL, 'Nullable(Array(Int32))'),
        CAST([], 'Nullable(Array(Int32))'),
        CAST([1, 2, 3], 'Nullable(Array(Int32))')
    ]) AS a
)
FORMAT Null;

SELECT throwIf(arrayReduce('sum', CAST([1, 2, 3], 'Nullable(Array(UInt8))')) != 6)
FORMAT Null;

SELECT throwIf(NOT isNull(arrayReduce('sum', CAST(NULL, 'Nullable(Array(UInt8))'))))
FORMAT Null;

SELECT throwIf(NOT isNull(arrayReduce('sumIf', CAST([] AS Nullable(Array(Int32))), CAST(NULL AS Nullable(Array(UInt8))))))
FORMAT Null;

SELECT throwIf(NOT isNull(arrayReduce('sumIf', CAST(NULL AS Nullable(Array(Int32))), CAST([] AS Nullable(Array(UInt8))))))
FORMAT Null;

SELECT throwIf(groupArray(isNull(arrayReduce('sum', a))) != [1, 0, 0])
FROM
(
    SELECT arrayJoin([
        CAST(NULL, 'Nullable(Array(Int32))'),
        CAST([], 'Nullable(Array(Int32))'),
        CAST([1, 2, 3], 'Nullable(Array(Int32))')
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayReduce('sum', a), -1)) != [-1, 0, 6])
FROM
(
    SELECT arrayJoin([
        CAST(NULL, 'Nullable(Array(Int32))'),
        CAST([], 'Nullable(Array(Int32))'),
        CAST([1, 2, 3], 'Nullable(Array(Int32))')
    ]) AS a
)
FORMAT Null;

SELECT throwIf(arrayFold(acc, x -> acc + x, CAST([1, 2, 3], 'Nullable(Array(UInt8))'), 0::UInt64) != 6)
FORMAT Null;

SELECT throwIf(arrayFold(acc, x -> acc + x, CAST([], 'Nullable(Array(UInt8))'), 10::UInt64) != 10)
FORMAT Null;

SELECT throwIf(NOT isNull(arrayFold(acc, x -> acc + x, CAST(NULL, 'Nullable(Array(UInt8))'), 0::UInt64)))
FORMAT Null;

SELECT throwIf(NOT isNull(arrayFold((acc, x, y) -> acc + x + y, CAST([] AS Nullable(Array(Int32))), CAST(NULL AS Nullable(Array(Int32))), 0::Int64)))
FORMAT Null;

SELECT throwIf(NOT isNull(arrayFold((acc, x, y) -> acc + x + y, CAST(NULL AS Nullable(Array(Int32))), CAST([] AS Nullable(Array(Int32))), 0::Int64)))
FORMAT Null;

SELECT throwIf(groupArray(isNull(arrayFold(acc, x -> acc + x, a, 0::Int64))) != [1, 0, 0])
FROM
(
    SELECT arrayJoin([
        CAST(NULL, 'Nullable(Array(Int32))'),
        CAST([], 'Nullable(Array(Int32))'),
        CAST([1, 2, 3], 'Nullable(Array(Int32))')
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayFold(acc, x -> acc + x, a, 0::Int64), -1)) != [-1, 0, 6])
FROM
(
    SELECT arrayJoin([
        CAST(NULL, 'Nullable(Array(Int32))'),
        CAST([], 'Nullable(Array(Int32))'),
        CAST([1, 2, 3], 'Nullable(Array(Int32))')
    ]) AS a
)
FORMAT Null;

SELECT 'ok';
