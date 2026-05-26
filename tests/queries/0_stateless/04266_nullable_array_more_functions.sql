-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 1;

SELECT throwIf(groupArray(ifNull(arrayConcat(a, b), [])) != [[1, 2, 3], [], []])
FROM
(
    SELECT arrayJoin([
        (CAST([1, 2] AS Nullable(Array(Int32))), CAST([3] AS Nullable(Array(Int32)))),
        (CAST(NULL AS Nullable(Array(Int32))), CAST(NULL AS Nullable(Array(Int32)))),
        (CAST([] AS Nullable(Array(Int32))), CAST([] AS Nullable(Array(Int32))))
    ]) AS t, t.1 AS a, t.2 AS b
)
FORMAT Null;

SELECT throwIf(groupArray(isNull(arrayZip(a, b))) != [0, 1, 0])
FROM
(
    SELECT arrayJoin([
        (CAST([1, 2] AS Nullable(Array(Int32))), CAST([3, 4] AS Nullable(Array(Int32)))),
        (CAST(NULL AS Nullable(Array(Int32))), CAST(NULL AS Nullable(Array(Int32)))),
        (CAST([] AS Nullable(Array(Int32))), CAST([] AS Nullable(Array(Int32))))
    ]) AS t, t.1 AS a, t.2 AS b
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(length(arrayZipUnaligned(a, b)), 99)) != [3, 99, 0])
FROM
(
    SELECT arrayJoin([
        (CAST([1, 2, 3] AS Nullable(Array(Int32))), CAST([4] AS Nullable(Array(Int32)))),
        (CAST(NULL AS Nullable(Array(Int32))), CAST(NULL AS Nullable(Array(Int32)))),
        (CAST([] AS Nullable(Array(Int32))), CAST([] AS Nullable(Array(Int32))))
    ]) AS t, t.1 AS a, t.2 AS b
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayMap(x -> x + 1, a), [])) != [[2, 3], [], []])
FROM
(
    SELECT arrayJoin([
        CAST([1, 2] AS Nullable(Array(Int32))),
        CAST(NULL AS Nullable(Array(Int32))),
        CAST([] AS Nullable(Array(Int32)))
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayFilter(x -> x > 1, a), [])) != [[2, 3], [], []])
FROM
(
    SELECT arrayJoin([
        CAST([1, 2, 3] AS Nullable(Array(Int32))),
        CAST(NULL AS Nullable(Array(Int32))),
        CAST([] AS Nullable(Array(Int32)))
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayExists(x -> x = 2, a), 99)) != [1, 99, 0])
FROM
(
    SELECT arrayJoin([
        CAST([1, 2] AS Nullable(Array(Int32))),
        CAST(NULL AS Nullable(Array(Int32))),
        CAST([] AS Nullable(Array(Int32)))
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayAll(x -> x > 0, a), 99)) != [1, 99, 1])
FROM
(
    SELECT arrayJoin([
        CAST([1, 2] AS Nullable(Array(Int32))),
        CAST(NULL AS Nullable(Array(Int32))),
        CAST([] AS Nullable(Array(Int32)))
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayCount(x -> x > 1, a), 99)) != [2, 99, 0])
FROM
(
    SELECT arrayJoin([
        CAST([1, 2, 3] AS Nullable(Array(Int32))),
        CAST(NULL AS Nullable(Array(Int32))),
        CAST([] AS Nullable(Array(Int32)))
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arraySort(a), [])) != [[1, 2, 3], [], []])
FROM
(
    SELECT arrayJoin([
        CAST([3, 1, 2] AS Nullable(Array(Int32))),
        CAST(NULL AS Nullable(Array(Int32))),
        CAST([] AS Nullable(Array(Int32)))
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayEnumerate(a), [])) != [[1, 2], [], []])
FROM
(
    SELECT arrayJoin([
        CAST([10, 20] AS Nullable(Array(Int32))),
        CAST(NULL AS Nullable(Array(Int32))),
        CAST([] AS Nullable(Array(Int32)))
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayEnumerateDense(a), [])) != [[1, 2, 1], [], []])
FROM
(
    SELECT arrayJoin([
        CAST([10, 20, 10] AS Nullable(Array(Int32))),
        CAST(NULL AS Nullable(Array(Int32))),
        CAST([] AS Nullable(Array(Int32)))
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayEnumerateUniq(a), [])) != [[1, 1, 2], [], []])
FROM
(
    SELECT arrayJoin([
        CAST([10, 20, 10] AS Nullable(Array(Int32))),
        CAST(NULL AS Nullable(Array(Int32))),
        CAST([] AS Nullable(Array(Int32)))
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayEnumerateUniqRanked(a), [])) != [[1, 1, 2], [], []])
FROM
(
    SELECT arrayJoin([
        CAST([10, 20, 10] AS Nullable(Array(Int32))),
        CAST(NULL AS Nullable(Array(Int32))),
        CAST([] AS Nullable(Array(Int32)))
    ]) AS a
)
FORMAT Null;

SELECT throwIf(groupArray(ifNull(arrayEnumerateDenseRanked(a), [])) != [[1, 2, 1], [], []])
FROM
(
    SELECT arrayJoin([
        CAST([10, 20, 10] AS Nullable(Array(Int32))),
        CAST(NULL AS Nullable(Array(Int32))),
        CAST([] AS Nullable(Array(Int32)))
    ]) AS a
)
FORMAT Null;

SELECT throwIf(arrayReduceInRanges(
    'sum',
    CAST([(1, 2), (2, 2)] AS Nullable(Array(Tuple(Int64, UInt64)))),
    CAST([10, 20, 30] AS Nullable(Array(Int32)))) != [30, 50])
FORMAT Null;

SELECT throwIf(NOT isNull(arrayReduceInRanges(
    'sum',
    CAST(NULL AS Nullable(Array(Tuple(Int64, UInt64)))),
    CAST([10, 20, 30] AS Nullable(Array(Int32))))))
FORMAT Null;

SELECT throwIf(NOT isNull(arrayReduceInRanges(
    'sum',
    CAST([(1, 2)] AS Nullable(Array(Tuple(Int64, UInt64)))),
    CAST(NULL AS Nullable(Array(Int32))))))
FORMAT Null;

DROP TABLE IF EXISTS nullable_array_join_more_functions;
CREATE TABLE nullable_array_join_more_functions (a Nullable(Array(Int32))) ENGINE = Memory;
INSERT INTO nullable_array_join_more_functions VALUES (NULL), ([]), ([1, 2]);
SELECT throwIf(count() != 2) FROM (SELECT arrayJoin(a) FROM nullable_array_join_more_functions) FORMAT Null;
DROP TABLE nullable_array_join_more_functions;

SELECT 'ok';
