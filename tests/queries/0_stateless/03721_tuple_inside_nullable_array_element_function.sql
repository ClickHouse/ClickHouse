-- { echoOn }

SET allow_experimental_nullable_tuple_type = 1;

SELECT toTypeName(arrayElementOrNull([(1, 'a'), (2, 'b')], 1));

SELECT toTypeName(arrayElementOrNull(CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))), 1));

SELECT toTypeName(arrayElementOrNull(CAST([(NULL, 'a'), (1, 'b')] AS Array(Tuple(Nullable(Int64), String))), 1));

SELECT toTypeName(arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Tuple())), 1));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], 1);

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], 2);

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], 3);

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], -1);

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], -2);

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], -3);

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], 0);

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(1 AS Int8));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(-1 AS Int8));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(2 AS UInt8));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(-2 AS Int16));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(1 AS Int16));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(2 AS UInt16));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(-2 AS Int32));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(1 AS Int32));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(2 AS UInt32));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(-2 AS Int64));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(1 AS Int64));

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], 1);

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], 2);

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], 3);

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], -1);

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], -2);

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], -3);

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], 0);

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(1 AS Int8));

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(-1 AS Int8));

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(2 AS UInt8));

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(-2 AS Int16));

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(1 AS Int16));

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(2 AS UInt16));

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(-2 AS Int32));

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(1 AS Int32));

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(2 AS UInt32));

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(-2 AS Int64));

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(1 AS Int64));

SELECT
    arrayElementOrNull(arr, 1),
    arrayElementOrNull(arr, -1),
    arrayElementOrNull(arr, 3),
    arrayElementOrNull(arr, 0),
    arrayElementOrNull(arr, CAST(1 AS Int8)),
    arrayElementOrNull(arr, CAST(-1 AS Int8)),
    arrayElementOrNull(arr, CAST(2 AS UInt8)),
    arrayElementOrNull(arr, CAST(-2 AS Int16)),
    arrayElementOrNull(arr, CAST(1 AS Int16)),
    arrayElementOrNull(arr, CAST(2 AS UInt16)),
    arrayElementOrNull(arr, CAST(-2 AS Int32)),
    arrayElementOrNull(arr, CAST(1 AS Int32)),
    arrayElementOrNull(arr, CAST(2 AS UInt32)),
    arrayElementOrNull(arr, CAST(-2 AS Int64)),
    arrayElementOrNull(arr, CAST(1 AS Int64)),
    arrayElementOrNull(arr, CAST(-1 AS Int64)),
    arrayElementOrNull(arr, CAST(1 AS Int64))
FROM
(
    SELECT [(1, 'a'), (2, 'b')] AS arr
    UNION ALL
    SELECT [(3, 'c')]           AS arr
    UNION ALL
    SELECT []                   AS arr
) ORDER BY tuple();

SELECT
    arrayElementOrNull(arr, 1),
    arrayElementOrNull(arr, -1),
    arrayElementOrNull(arr, 3),
    arrayElementOrNull(arr, 0),
    arrayElementOrNull(arr, CAST(1 AS Int8)),
    arrayElementOrNull(arr, CAST(-1 AS Int8)),
    arrayElementOrNull(arr, CAST(2 AS UInt8)),
    arrayElementOrNull(arr, CAST(-2 AS Int16)),
    arrayElementOrNull(arr, CAST(1 AS Int16)),
    arrayElementOrNull(arr, CAST(2 AS UInt16)),
    arrayElementOrNull(arr, CAST(-2 AS Int32)),
    arrayElementOrNull(arr, CAST(1 AS Int32)),
    arrayElementOrNull(arr, CAST(2 AS UInt32)),
    arrayElementOrNull(arr, CAST(-2 AS Int64)),
    arrayElementOrNull(arr, CAST(1 AS Int64)),
    arrayElementOrNull(arr, CAST(-1 AS Int64)),
    arrayElementOrNull(arr, CAST(1 AS Int64))
FROM
(
    SELECT [CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')] AS arr
    UNION ALL
    SELECT [CAST((3, 'c') AS Nullable(Tuple(Int64, String)))]           AS arr
    UNION ALL
    SELECT [NULL]                   AS arr
) ORDER BY tuple();


SELECT arrayElementOrNull(arr, idx)
FROM
(
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, 1  AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, 2  AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, 3  AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, -1 AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, -2 AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, -3 AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, 0  AS idx
) ORDER BY tuple();

WITH CAST([(NULL, 'a'), (1, 'b')] AS Array(Tuple(Nullable(Int64), String))) AS arr
SELECT
    arrayElementOrNull(arr, 1) AS idx1,
    arrayElementOrNull(arr, 2) AS idx2,
    arrayElementOrNull(arr, 3) AS idx3,
    toTypeName(arrayElementOrNull(arr, 1)) AS type1;

SELECT
    arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Tuple())), 1) AS idx1,
    arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Tuple())), 2) AS idx2,
    arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Tuple())), 3) AS idx3,
    toTypeName(arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Tuple())), 1)) AS type1;

SELECT
    arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))), 1) AS idx1,
    arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))), 2) AS idx2,
    arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))), 3) AS idx3,
    toTypeName(arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))), 1)) AS type1;

SELECT arrayElementOrNull(arr, idx)
FROM
(
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr,  1 AS idx
    UNION ALL
    SELECT CAST([NULL]           AS Array(Nullable(Tuple(Int64, String)))) AS arr,  1 AS idx
    UNION ALL
    SELECT CAST([]               AS Array(Nullable(Tuple(Int64, String)))) AS arr,  1 AS idx
    UNION ALL
    SELECT CAST([(2, 'b')]       AS Array(Nullable(Tuple(Int64, String)))) AS arr,  2 AS idx
    UNION ALL
    SELECT CAST([(3, 'c')]       AS Array(Nullable(Tuple(Int64, String)))) AS arr, -1 AS idx
    UNION ALL
    SELECT CAST([]               AS Array(Nullable(Tuple(Int64, String)))) AS arr, -1 AS idx
) ORDER BY tuple();

SELECT arrayElementOrNull(arr, idx)
FROM
(
    SELECT CAST([tuple(), tuple()] AS Array(Tuple())) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([tuple()]          AS Array(Tuple())) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([]                 AS Array(Tuple())) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([tuple(), tuple()] AS Array(Tuple())) AS arr, 3 AS idx
) ORDER BY tuple();


SELECT arrayElementOrNull(arr, idx)
FROM
(
    SELECT CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([tuple()]          AS Array(Nullable(Tuple()))) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([]                 AS Array(Nullable(Tuple()))) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([NULL]             AS Array(Nullable(Tuple()))) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))) AS arr, 3 AS idx
) ORDER BY tuple();

WITH [(1, 'a'), (2, 'b')] AS arr
SELECT arrayElementOrNull(arr, idx)
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
    UNION ALL
    SELECT 0  AS idx
) ORDER BY tuple();

WITH CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr
SELECT
    idx,
    arrayElementOrNull(arr, idx) AS value,
    toTypeName(arrayElementOrNull(arr, idx)) AS type
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
    UNION ALL
    SELECT 0  AS idx
) ORDER BY tuple();

WITH [(1, 'a'), (2, 'b')] AS arr
SELECT
    arrayElementOrNull(arr, CAST(idx AS Int8))  AS int8_res,
    arrayElementOrNull(arr, CAST(idx AS UInt8)) AS uint8_res
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
) ORDER BY tuple();

WITH CAST([tuple(), tuple()] AS Array(Tuple())) AS arr
SELECT
    idx,
    arrayElementOrNull(arr, idx)                    AS value,
    toTypeName(arrayElementOrNull(arr, idx))        AS type
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
) ORDER BY tuple();

WITH CAST([tuple(), NULL] AS Array(Nullable(Tuple()))) AS arr
SELECT
    idx,
    arrayElementOrNull(arr, idx)                    AS value,
    toTypeName(arrayElementOrNull(arr, idx))        AS type
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
) ORDER BY tuple();

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], 'x'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

DROP TABLE IF EXISTS test_array_tuple_mergetree;

CREATE TABLE test_array_tuple_mergetree
(
    id       UInt8,
    arr      Array(Tuple(Int64, String)),
    arr_null Array(Nullable(Tuple(Int64, String))),
    idx      Int64
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test_array_tuple_mergetree VALUES
    (1, [(1, 'a'), (2, 'b')], [(1, 'a'), NULL], 1),
    (2, [(3, 'c')],           [NULL],           2),
    (3, [],                   [],               -1),
    (4, [(4, 'd')],           [(4, 'd')],       0);

SELECT
    id,
    arrayElementOrNull(arr, 1)        AS arr_1,
    arrayElementOrNull(arr, -1)       AS arr_minus_1,
    arrayElementOrNull(arr, 3)        AS arr_3,
    arrayElementOrNull(arr_null, 1)   AS arr_null_1,
    arrayElementOrNull(arr_null, 2)   AS arr_null_2,
    arrayElementOrNull(arr_null, 3)   AS arr_null_3
FROM test_array_tuple_mergetree
ORDER BY id;

SELECT
    id,
    idx,
    arrayElementOrNull(arr, idx)      AS arr_idx,
    arrayElementOrNull(arr_null, idx) AS arr_null_idx
FROM test_array_tuple_mergetree
ORDER BY id;


SELECT toTypeName(arrayElement([(1, 'a'), (2, 'b')], 1));

SELECT toTypeName(arrayElement(CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))), 1));

SELECT toTypeName(arrayElement(CAST([(NULL, 'a'), (1, 'b')] AS Array(Tuple(Nullable(Int64), String))), 1));

SELECT toTypeName(arrayElement(CAST([tuple(), tuple()] AS Array(Tuple())), 1));

SELECT arrayElement([(1, 'a'), (2, 'b')], 1);

SELECT arrayElement([(1, 'a'), (2, 'b')], 2);

SELECT arrayElement([(1, 'a'), (2, 'b')], 3);

SELECT arrayElement([(1, 'a'), (2, 'b')], -1);

SELECT arrayElement([(1, 'a'), (2, 'b')], -2);

SELECT arrayElement([(1, 'a'), (2, 'b')], -3);

SELECT arrayElement([(1, 'a'), (2, 'b')], 0);

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(1 AS Int8));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(-1 AS Int8));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(2 AS UInt8));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(-2 AS Int16));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(1 AS Int16));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(2 AS UInt16));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(-2 AS Int32));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(1 AS Int32));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(2 AS UInt32));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(-2 AS Int64));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(1 AS Int64));

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], 1);

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], 2);

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], 3);

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], -1);

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], -2);

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], -3);

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], 0);

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(1 AS Int8));

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(-1 AS Int8));

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(2 AS UInt8));

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(-2 AS Int16));

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(1 AS Int16));

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(2 AS UInt16));

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(-2 AS Int32));

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(1 AS Int32));

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(2 AS UInt32));

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(-2 AS Int64));

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], CAST(1 AS Int64));

SELECT arrayElement([(1, 'a'), (2, 'b')], 0);
SELECT
    arrayElement(arr, 1),
    arrayElement(arr, -1),
    arrayElement(arr, 3),
    arrayElement(arr, CAST(1 AS Int8)),
    arrayElement(arr, CAST(-1 AS Int8)),
    arrayElement(arr, CAST(2 AS UInt8)),
    arrayElement(arr, CAST(-2 AS Int16)),
    arrayElement(arr, CAST(1 AS Int16)),
    arrayElement(arr, CAST(2 AS UInt16)),
    arrayElement(arr, CAST(-2 AS Int32)),
    arrayElement(arr, CAST(1 AS Int32)),
    arrayElement(arr, CAST(2 AS UInt32)),
    arrayElement(arr, CAST(-2 AS Int64)),
    arrayElement(arr, CAST(1 AS Int64)),
    arrayElement(arr, CAST(-1 AS Int64)),
    arrayElement(arr, CAST(1 AS Int64))
FROM
(
    SELECT [(1, 'a'), (2, 'b')] AS arr
    UNION ALL
    SELECT [(3, 'c')]           AS arr
    UNION ALL
    SELECT []                   AS arr
) ORDER BY tuple();


SELECT
    arrayElement(arr, 0),
    arrayElement(arr, -1),
FROM
(
    SELECT [(1, 'a'), (2, 'b')] AS arr
    UNION ALL
    SELECT [(3, 'c')]           AS arr
    UNION ALL
    SELECT []                   AS arr
); -- {serverError ZERO_ARRAY_OR_TUPLE_INDEX}

SELECT
    arrayElement(arr, 0),
    arrayElement(arr, -1),
FROM
(
    SELECT [(1, 'a'), (2, 'b')] AS arr
    UNION ALL
    SELECT [(3, 'c')]           AS arr
    UNION ALL
    SELECT [NULL]                   AS arr
); -- {serverError ZERO_ARRAY_OR_TUPLE_INDEX}

SELECT
    arrayElement(arr, 1),
    arrayElement(arr, -1),
    arrayElement(arr, 3),
    arrayElement(arr, CAST(1 AS Int8)),
    arrayElement(arr, CAST(-1 AS Int8)),
    arrayElement(arr, CAST(2 AS UInt8)),
    arrayElement(arr, CAST(-2 AS Int16)),
    arrayElement(arr, CAST(1 AS Int16)),
    arrayElement(arr, CAST(2 AS UInt16)),
    arrayElement(arr, CAST(-2 AS Int32)),
    arrayElement(arr, CAST(1 AS Int32)),
    arrayElement(arr, CAST(2 AS UInt32)),
    arrayElement(arr, CAST(-2 AS Int64)),
    arrayElement(arr, CAST(1 AS Int64)),
    arrayElement(arr, CAST(-1 AS Int64)),
    arrayElement(arr, CAST(1 AS Int64))
FROM
(
    SELECT [CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')] AS arr
    UNION ALL
    SELECT [CAST((3, 'c') AS Nullable(Tuple(Int64, String)))]           AS arr
    UNION ALL
    SELECT [NULL]                   AS arr
) ORDER BY tuple();


SELECT arrayElement(arr, idx)
FROM
(
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, 1  AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, 2  AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, 3  AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, -1 AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, -2 AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, -3 AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, 0  AS idx
) ORDER BY tuple();

WITH CAST([(NULL, 'a'), (1, 'b')] AS Array(Tuple(Nullable(Int64), String))) AS arr
SELECT
    arrayElement(arr, 1) AS idx1,
    arrayElement(arr, 2) AS idx2,
    arrayElement(arr, 3) AS idx3,
    toTypeName(arrayElement(arr, 1)) AS type1;

SELECT
    arrayElement(CAST([tuple(), tuple()] AS Array(Tuple())), 1) AS idx1,
    arrayElement(CAST([tuple(), tuple()] AS Array(Tuple())), 2) AS idx2,
    arrayElement(CAST([tuple(), tuple()] AS Array(Tuple())), 3) AS idx3,
    toTypeName(arrayElement(CAST([tuple(), tuple()] AS Array(Tuple())), 1)) AS type1;

SELECT
    arrayElement(CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))), 1) AS idx1,
    arrayElement(CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))), 2) AS idx2,
    arrayElement(CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))), 3) AS idx3,
    toTypeName(arrayElement(CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))), 1)) AS type1;

SELECT arrayElement(arr, idx)
FROM
(
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr,  1 AS idx
    UNION ALL
    SELECT CAST([NULL]           AS Array(Nullable(Tuple(Int64, String)))) AS arr,  1 AS idx
    UNION ALL
    SELECT CAST([]               AS Array(Nullable(Tuple(Int64, String)))) AS arr,  1 AS idx
    UNION ALL
    SELECT CAST([(2, 'b')]       AS Array(Nullable(Tuple(Int64, String)))) AS arr,  2 AS idx
    UNION ALL
    SELECT CAST([(3, 'c')]       AS Array(Nullable(Tuple(Int64, String)))) AS arr, -1 AS idx
    UNION ALL
    SELECT CAST([]               AS Array(Nullable(Tuple(Int64, String)))) AS arr, -1 AS idx
) ORDER BY tuple();

SELECT arrayElement(arr, idx)
FROM
(
    SELECT CAST([tuple(), tuple()] AS Array(Tuple())) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([tuple()]          AS Array(Tuple())) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([]                 AS Array(Tuple())) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([tuple(), tuple()] AS Array(Tuple())) AS arr, 3 AS idx
) ORDER BY tuple();


SELECT arrayElement(arr, idx)
FROM
(
    SELECT CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([tuple()]          AS Array(Nullable(Tuple()))) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([]                 AS Array(Nullable(Tuple()))) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([NULL]             AS Array(Nullable(Tuple()))) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))) AS arr, 3 AS idx
) ORDER BY tuple();

WITH [(1, 'a'), (2, 'b')] AS arr
SELECT arrayElement(arr, idx)
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
    UNION ALL
    SELECT 0  AS idx
) ORDER BY tuple();

WITH CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr
SELECT
    idx,
    arrayElement(arr, idx) AS value,
    toTypeName(arrayElement(arr, idx)) AS type
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
    UNION ALL
    SELECT 0  AS idx
) ORDER BY tuple();

WITH [(1, 'a'), (2, 'b')] AS arr
SELECT
    arrayElement(arr, CAST(idx AS Int8))  AS int8_res,
    arrayElement(arr, CAST(idx AS UInt8)) AS uint8_res
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
) ORDER BY tuple();

WITH CAST([tuple(), tuple()] AS Array(Tuple())) AS arr
SELECT
    idx,
    arrayElement(arr, idx)                    AS value,
    toTypeName(arrayElement(arr, idx))        AS type
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
) ORDER BY tuple();

WITH CAST([tuple(), NULL] AS Array(Nullable(Tuple()))) AS arr
SELECT
    idx,
    arrayElement(arr, idx)                    AS value,
    toTypeName(arrayElement(arr, idx))        AS type
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
) ORDER BY tuple();

SELECT arrayElement([(1, 'a'), (2, 'b')], 'x'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

DROP TABLE IF EXISTS test_array_tuple_mergetree;

CREATE TABLE test_array_tuple_mergetree
(
    id       UInt8,
    arr      Array(Tuple(Int64, String)),
    arr_null Array(Nullable(Tuple(Int64, String))),
    idx      Int64
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test_array_tuple_mergetree VALUES
    (1, [(1, 'a'), (2, 'b')], [(1, 'a'), NULL], 1),
    (2, [(3, 'c')],           [NULL],           2),
    (3, [],                   [],               -1),
    (4, [(4, 'd')],           [(4, 'd')],       0);

SELECT
    id,
    arrayElement(arr, 1)        AS arr_1,
    arrayElement(arr, -1)       AS arr_minus_1,
    arrayElement(arr, 3)        AS arr_3,
    arrayElement(arr_null, 1)   AS arr_null_1,
    arrayElement(arr_null, 2)   AS arr_null_2,
    arrayElement(arr_null, 3)   AS arr_null_3
FROM test_array_tuple_mergetree
ORDER BY id;

SELECT
    id,
    idx,
    arrayElement(arr, idx)      AS arr_idx,
    arrayElement(arr_null, idx) AS arr_null_idx
FROM test_array_tuple_mergetree
ORDER BY id;

SELECT arrayElement([(1, 2)], NULL);

SELECT arrayElementOrNull([(1, 2)], NULL);

SELECT arrayElementOrNull([CAST(NULL AS Nullable(Tuple()))], NULL);

SELECT arrayElementOrNull([CAST(NULL AS Nullable(Tuple()))], NULL);

WITH [(1, 'a'), (2, 'b')] AS arr
SELECT
    idx,
    arrayElementOrNull(arr, idx) AS value,
    toTypeName(arrayElementOrNull(arr, idx)) AS type
FROM
(
    SELECT CAST(1    AS Nullable(Int64)) AS idx
    UNION ALL
    SELECT CAST(2    AS Nullable(Int64)) AS idx
    UNION ALL
    SELECT CAST(-1    AS Nullable(Int64)) AS idx
    UNION ALL
    SELECT CAST(-2    AS Nullable(Int64)) AS idx
    UNION ALL
    SELECT CAST(NULL AS Nullable(Int64)) AS idx
) ORDER BY tuple();

WITH CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr
SELECT
    idx,
    arrayElementOrNull(arr, idx) AS value,
    toTypeName(arrayElementOrNull(arr, idx)) AS type
FROM
(
    SELECT CAST(1    AS Nullable(Int64)) AS idx
    UNION ALL
    SELECT CAST(2    AS Nullable(Int64)) AS idx
    UNION ALL
    SELECT CAST(-1    AS Nullable(Int64)) AS idx
    UNION ALL
    SELECT CAST(-2    AS Nullable(Int64)) AS idx
    UNION ALL
    SELECT CAST(NULL AS Nullable(Int64)) AS idx
) ORDER BY tuple();



SET allow_experimental_nullable_tuple_type = 0;

SELECT toTypeName(arrayElementOrNull([(1, 'a'), (2, 'b')], 1));

SELECT toTypeName(arrayElementOrNull(CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))), 1)); -- { serverError ILLEGAL_COLUMN }

SELECT toTypeName(arrayElementOrNull(CAST([(NULL, 'a'), (1, 'b')] AS Array(Tuple(Nullable(Int64), String))), 1));

SELECT toTypeName(arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Tuple())), 1));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], 1);

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], 2);

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], 3);

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], -1);

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], -2);

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], -3);

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], 0);

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(1 AS Int8));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(-1 AS Int8));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(2 AS UInt8));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(-2 AS Int16));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(1 AS Int16));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(2 AS UInt16));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(-2 AS Int32));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(1 AS Int32));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(2 AS UInt32));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(-2 AS Int64));

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], CAST(1 AS Int64));

SELECT arrayElementOrNull([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], 1); -- { serverError ILLEGAL_COLUMN }

SELECT
    arrayElementOrNull(arr, 1),
    arrayElementOrNull(arr, -1),
    arrayElementOrNull(arr, 3),
    arrayElementOrNull(arr, 0),
    arrayElementOrNull(arr, CAST(1 AS Int8)),
    arrayElementOrNull(arr, CAST(-1 AS Int8)),
    arrayElementOrNull(arr, CAST(2 AS UInt8)),
    arrayElementOrNull(arr, CAST(-2 AS Int16)),
    arrayElementOrNull(arr, CAST(1 AS Int16)),
    arrayElementOrNull(arr, CAST(2 AS UInt16)),
    arrayElementOrNull(arr, CAST(-2 AS Int32)),
    arrayElementOrNull(arr, CAST(1 AS Int32)),
    arrayElementOrNull(arr, CAST(2 AS UInt32)),
    arrayElementOrNull(arr, CAST(-2 AS Int64)),
    arrayElementOrNull(arr, CAST(1 AS Int64)),
    arrayElementOrNull(arr, CAST(-1 AS Int64)),
    arrayElementOrNull(arr, CAST(1 AS Int64))
FROM
(
    SELECT [(1, 'a'), (2, 'b')] AS arr
    UNION ALL
    SELECT [(3, 'c')]           AS arr
    UNION ALL
    SELECT []                   AS arr
) ORDER BY tuple();

SELECT
    arrayElementOrNull(arr, 1),
    arrayElementOrNull(arr, -1),
    arrayElementOrNull(arr, 3),
    arrayElementOrNull(arr, 0),
    arrayElementOrNull(arr, CAST(1 AS Int8)),
    arrayElementOrNull(arr, CAST(-1 AS Int8)),
    arrayElementOrNull(arr, CAST(2 AS UInt8)),
    arrayElementOrNull(arr, CAST(-2 AS Int16)),
    arrayElementOrNull(arr, CAST(1 AS Int16)),
    arrayElementOrNull(arr, CAST(2 AS UInt16)),
    arrayElementOrNull(arr, CAST(-2 AS Int32)),
    arrayElementOrNull(arr, CAST(1 AS Int32)),
    arrayElementOrNull(arr, CAST(2 AS UInt32)),
    arrayElementOrNull(arr, CAST(-2 AS Int64)),
    arrayElementOrNull(arr, CAST(1 AS Int64)),
    arrayElementOrNull(arr, CAST(-1 AS Int64)),
    arrayElementOrNull(arr, CAST(1 AS Int64))
FROM
(
    SELECT [CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')] AS arr
    UNION ALL
    SELECT [CAST((3, 'c') AS Nullable(Tuple(Int64, String)))]           AS arr
    UNION ALL
    SELECT [NULL]                   AS arr
) ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }


SELECT arrayElementOrNull(arr, idx)
FROM
(
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, 1  AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, 2  AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, 3  AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, -1 AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, -2 AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, -3 AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, 0  AS idx
) ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

WITH CAST([(NULL, 'a'), (1, 'b')] AS Array(Tuple(Nullable(Int64), String))) AS arr
SELECT
    arrayElementOrNull(arr, 1) AS idx1,
    arrayElementOrNull(arr, 2) AS idx2,
    arrayElementOrNull(arr, 3) AS idx3,
    toTypeName(arrayElementOrNull(arr, 1)) AS type1;

SELECT
    arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Tuple())), 1) AS idx1,
    arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Tuple())), 2) AS idx2,
    arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Tuple())), 3) AS idx3,
    toTypeName(arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Tuple())), 1)) AS type1;

SELECT
    arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))), 1) AS idx1,
    arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))), 2) AS idx2,
    arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))), 3) AS idx3,
    toTypeName(arrayElementOrNull(CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))), 1)) AS type1; -- { serverError ILLEGAL_COLUMN }

SELECT arrayElementOrNull(arr, idx)
FROM
(
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr,  1 AS idx
    UNION ALL
    SELECT CAST([NULL]           AS Array(Nullable(Tuple(Int64, String)))) AS arr,  1 AS idx
    UNION ALL
    SELECT CAST([]               AS Array(Nullable(Tuple(Int64, String)))) AS arr,  1 AS idx
    UNION ALL
    SELECT CAST([(2, 'b')]       AS Array(Nullable(Tuple(Int64, String)))) AS arr,  2 AS idx
    UNION ALL
    SELECT CAST([(3, 'c')]       AS Array(Nullable(Tuple(Int64, String)))) AS arr, -1 AS idx
    UNION ALL
    SELECT CAST([]               AS Array(Nullable(Tuple(Int64, String)))) AS arr, -1 AS idx
) ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

SELECT arrayElementOrNull(arr, idx)
FROM
(
    SELECT CAST([tuple(), tuple()] AS Array(Tuple())) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([tuple()]          AS Array(Tuple())) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([]                 AS Array(Tuple())) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([tuple(), tuple()] AS Array(Tuple())) AS arr, 3 AS idx
) ORDER BY tuple();


SELECT arrayElementOrNull(arr, idx)
FROM
(
    SELECT CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([tuple()]          AS Array(Nullable(Tuple()))) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([]                 AS Array(Nullable(Tuple()))) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([NULL]             AS Array(Nullable(Tuple()))) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))) AS arr, 3 AS idx
) ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

WITH [(1, 'a'), (2, 'b')] AS arr
SELECT arrayElementOrNull(arr, idx)
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
    UNION ALL
    SELECT 0  AS idx
) ORDER BY tuple();

WITH CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr
SELECT
    idx,
    arrayElementOrNull(arr, idx) AS value,
    toTypeName(arrayElementOrNull(arr, idx)) AS type
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
    UNION ALL
    SELECT 0  AS idx
) ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

WITH [(1, 'a'), (2, 'b')] AS arr
SELECT
    arrayElementOrNull(arr, CAST(idx AS Int8))  AS int8_res,
    arrayElementOrNull(arr, CAST(idx AS UInt8)) AS uint8_res
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
) ORDER BY tuple();

WITH CAST([tuple(), tuple()] AS Array(Tuple())) AS arr
SELECT
    idx,
    arrayElementOrNull(arr, idx)                    AS value,
    toTypeName(arrayElementOrNull(arr, idx))        AS type
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
) ORDER BY tuple();

WITH CAST([tuple(), NULL] AS Array(Nullable(Tuple()))) AS arr
SELECT
    idx,
    arrayElementOrNull(arr, idx)                    AS value,
    toTypeName(arrayElementOrNull(arr, idx))        AS type
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
) ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

SELECT arrayElementOrNull([(1, 'a'), (2, 'b')], 'x'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT toTypeName(arrayElement([(1, 'a'), (2, 'b')], 1));

SELECT toTypeName(arrayElement(CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))), 1)); -- { serverError ILLEGAL_COLUMN }

SELECT toTypeName(arrayElement(CAST([(NULL, 'a'), (1, 'b')] AS Array(Tuple(Nullable(Int64), String))), 1));

SELECT toTypeName(arrayElement(CAST([tuple(), tuple()] AS Array(Tuple())), 1));

SELECT arrayElement([(1, 'a'), (2, 'b')], 1);

SELECT arrayElement([(1, 'a'), (2, 'b')], 2);

SELECT arrayElement([(1, 'a'), (2, 'b')], 3);

SELECT arrayElement([(1, 'a'), (2, 'b')], -1);

SELECT arrayElement([(1, 'a'), (2, 'b')], -2);

SELECT arrayElement([(1, 'a'), (2, 'b')], -3);

SELECT arrayElement([(1, 'a'), (2, 'b')], 0);

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(1 AS Int8));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(-1 AS Int8));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(2 AS UInt8));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(-2 AS Int16));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(1 AS Int16));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(2 AS UInt16));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(-2 AS Int32));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(1 AS Int32));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(2 AS UInt32));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(-2 AS Int64));

SELECT arrayElement([(1, 'a'), (2, 'b')], CAST(1 AS Int64));

SELECT arrayElement([CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')], 1); -- { serverError ILLEGAL_COLUMN }

SELECT arrayElement([(1, 'a'), (2, 'b')], 0);
SELECT
    arrayElement(arr, 1),
    arrayElement(arr, -1),
    arrayElement(arr, 3),
    arrayElement(arr, CAST(1 AS Int8)),
    arrayElement(arr, CAST(-1 AS Int8)),
    arrayElement(arr, CAST(2 AS UInt8)),
    arrayElement(arr, CAST(-2 AS Int16)),
    arrayElement(arr, CAST(1 AS Int16)),
    arrayElement(arr, CAST(2 AS UInt16)),
    arrayElement(arr, CAST(-2 AS Int32)),
    arrayElement(arr, CAST(1 AS Int32)),
    arrayElement(arr, CAST(2 AS UInt32)),
    arrayElement(arr, CAST(-2 AS Int64)),
    arrayElement(arr, CAST(1 AS Int64)),
    arrayElement(arr, CAST(-1 AS Int64)),
    arrayElement(arr, CAST(1 AS Int64))
FROM
(
    SELECT [(1, 'a'), (2, 'b')] AS arr
    UNION ALL
    SELECT [(3, 'c')]           AS arr
    UNION ALL
    SELECT []                   AS arr
) ORDER BY tuple();


SELECT
    arrayElement(arr, 0),
    arrayElement(arr, -1),
FROM
(
    SELECT [(1, 'a'), (2, 'b')] AS arr
    UNION ALL
    SELECT [(3, 'c')]           AS arr
    UNION ALL
    SELECT []                   AS arr
); -- {serverError ZERO_ARRAY_OR_TUPLE_INDEX}

SELECT
    arrayElement(arr, 0),
    arrayElement(arr, -1),
FROM
(
    SELECT [(1, 'a'), (2, 'b')] AS arr
    UNION ALL
    SELECT [(3, 'c')]           AS arr
    UNION ALL
    SELECT [NULL]                   AS arr
); -- {serverError ZERO_ARRAY_OR_TUPLE_INDEX}

SELECT
    arrayElement(arr, 1),
    arrayElement(arr, -1),
    arrayElement(arr, 3),
    arrayElement(arr, CAST(1 AS Int8)),
    arrayElement(arr, CAST(-1 AS Int8)),
    arrayElement(arr, CAST(2 AS UInt8)),
    arrayElement(arr, CAST(-2 AS Int16)),
    arrayElement(arr, CAST(1 AS Int16)),
    arrayElement(arr, CAST(2 AS UInt16)),
    arrayElement(arr, CAST(-2 AS Int32)),
    arrayElement(arr, CAST(1 AS Int32)),
    arrayElement(arr, CAST(2 AS UInt32)),
    arrayElement(arr, CAST(-2 AS Int64)),
    arrayElement(arr, CAST(1 AS Int64)),
    arrayElement(arr, CAST(-1 AS Int64)),
    arrayElement(arr, CAST(1 AS Int64))
FROM
(
    SELECT [CAST((1, 'a') AS Nullable(Tuple(Int64, String))), (2, 'b')] AS arr
    UNION ALL
    SELECT [CAST((3, 'c') AS Nullable(Tuple(Int64, String)))]           AS arr
    UNION ALL
    SELECT [NULL]                   AS arr
) ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }


SELECT arrayElement(arr, idx)
FROM
(
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, 1  AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, 2  AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, 3  AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, -1 AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, -2 AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, -3 AS idx
    UNION ALL
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr, 0  AS idx
) ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

WITH CAST([(NULL, 'a'), (1, 'b')] AS Array(Tuple(Nullable(Int64), String))) AS arr
SELECT
    arrayElement(arr, 1) AS idx1,
    arrayElement(arr, 2) AS idx2,
    arrayElement(arr, 3) AS idx3,
    toTypeName(arrayElement(arr, 1)) AS type1;

SELECT
    arrayElement(CAST([tuple(), tuple()] AS Array(Tuple())), 1) AS idx1,
    arrayElement(CAST([tuple(), tuple()] AS Array(Tuple())), 2) AS idx2,
    arrayElement(CAST([tuple(), tuple()] AS Array(Tuple())), 3) AS idx3,
    toTypeName(arrayElement(CAST([tuple(), tuple()] AS Array(Tuple())), 1)) AS type1;

SELECT
    arrayElement(CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))), 1) AS idx1,
    arrayElement(CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))), 2) AS idx2,
    arrayElement(CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))), 3) AS idx3,
    toTypeName(arrayElement(CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))), 1)) AS type1; -- { serverError ILLEGAL_COLUMN }

SELECT arrayElement(arr, idx)
FROM
(
    SELECT CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr,  1 AS idx
    UNION ALL
    SELECT CAST([NULL]           AS Array(Nullable(Tuple(Int64, String)))) AS arr,  1 AS idx
    UNION ALL
    SELECT CAST([]               AS Array(Nullable(Tuple(Int64, String)))) AS arr,  1 AS idx
    UNION ALL
    SELECT CAST([(2, 'b')]       AS Array(Nullable(Tuple(Int64, String)))) AS arr,  2 AS idx
    UNION ALL
    SELECT CAST([(3, 'c')]       AS Array(Nullable(Tuple(Int64, String)))) AS arr, -1 AS idx
    UNION ALL
    SELECT CAST([]               AS Array(Nullable(Tuple(Int64, String)))) AS arr, -1 AS idx
) ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

SELECT arrayElement(arr, idx)
FROM
(
    SELECT CAST([tuple(), tuple()] AS Array(Tuple())) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([tuple()]          AS Array(Tuple())) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([]                 AS Array(Tuple())) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([tuple(), tuple()] AS Array(Tuple())) AS arr, 3 AS idx
) ORDER BY tuple();


SELECT arrayElement(arr, idx)
FROM
(
    SELECT CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([tuple()]          AS Array(Nullable(Tuple()))) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([]                 AS Array(Nullable(Tuple()))) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([NULL]             AS Array(Nullable(Tuple()))) AS arr, 1 AS idx
    UNION ALL
    SELECT CAST([tuple(), tuple()] AS Array(Nullable(Tuple()))) AS arr, 3 AS idx
) ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

WITH [(1, 'a'), (2, 'b')] AS arr
SELECT arrayElement(arr, idx)
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
    UNION ALL
    SELECT 0  AS idx
) ORDER BY tuple();

WITH CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr
SELECT
    idx,
    arrayElement(arr, idx) AS value,
    toTypeName(arrayElement(arr, idx)) AS type
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
    UNION ALL
    SELECT 0  AS idx
) ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

WITH [(1, 'a'), (2, 'b')] AS arr
SELECT
    arrayElement(arr, CAST(idx AS Int8))  AS int8_res,
    arrayElement(arr, CAST(idx AS UInt8)) AS uint8_res
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
) ORDER BY tuple();

WITH CAST([tuple(), tuple()] AS Array(Tuple())) AS arr
SELECT
    idx,
    arrayElement(arr, idx)                    AS value,
    toTypeName(arrayElement(arr, idx))        AS type
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
) ORDER BY tuple();

WITH CAST([tuple(), NULL] AS Array(Nullable(Tuple()))) AS arr
SELECT
    idx,
    arrayElement(arr, idx)                    AS value,
    toTypeName(arrayElement(arr, idx))        AS type
FROM
(
    SELECT 1  AS idx
    UNION ALL
    SELECT 2  AS idx
    UNION ALL
    SELECT 3  AS idx
    UNION ALL
    SELECT -1 AS idx
    UNION ALL
    SELECT -2 AS idx
    UNION ALL
    SELECT -3 AS idx
) ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }

SELECT arrayElement([(1, 'a'), (2, 'b')], 'x'); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT arrayElement([(1, 2)], NULL);

SELECT arrayElementOrNull([(1, 2)], NULL);

SELECT arrayElementOrNull([CAST(NULL AS Nullable(Tuple()))], NULL); -- { serverError ILLEGAL_COLUMN }

WITH [(1, 'a'), (2, 'b')] AS arr
SELECT
    idx,
    arrayElementOrNull(arr, idx) AS value,
    toTypeName(arrayElementOrNull(arr, idx)) AS type
FROM
(
    SELECT CAST(1    AS Nullable(Int64)) AS idx
    UNION ALL
    SELECT CAST(2    AS Nullable(Int64)) AS idx
    UNION ALL
    SELECT CAST(-1    AS Nullable(Int64)) AS idx
    UNION ALL
    SELECT CAST(-2    AS Nullable(Int64)) AS idx
    UNION ALL
    SELECT CAST(NULL AS Nullable(Int64)) AS idx
) ORDER BY tuple();

WITH CAST([(1, 'a'), NULL] AS Array(Nullable(Tuple(Int64, String)))) AS arr
SELECT
    idx,
    arrayElementOrNull(arr, idx) AS value,
    toTypeName(arrayElementOrNull(arr, idx)) AS type
FROM
(
    SELECT CAST(1    AS Nullable(Int64)) AS idx
    UNION ALL
    SELECT CAST(2    AS Nullable(Int64)) AS idx
    UNION ALL
    SELECT CAST(-1    AS Nullable(Int64)) AS idx
    UNION ALL
    SELECT CAST(-2    AS Nullable(Int64)) AS idx
    UNION ALL
    SELECT CAST(NULL AS Nullable(Int64)) AS idx
) ORDER BY tuple(); -- { serverError ILLEGAL_COLUMN }
