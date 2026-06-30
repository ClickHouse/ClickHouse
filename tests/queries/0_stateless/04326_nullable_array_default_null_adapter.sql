-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 1;
SET short_circuit_function_evaluation_for_nulls = 0;

SELECT throwIf(toTypeName(arrayReverse(CAST(NULL AS Nullable(Array(UInt8))))) != 'Nullable(Array(UInt8))')
FORMAT Null;

SELECT throwIf(NOT isNull(arrayReverse(CAST(NULL AS Nullable(Array(UInt8))))))
FORMAT Null;

SELECT throwIf(toTypeName(arrayDistinct(CAST(NULL AS Nullable(Array(UInt8))))) != 'Nullable(Array(UInt8))')
FORMAT Null;

SELECT throwIf(NOT isNull(arrayDistinct(CAST(NULL AS Nullable(Array(UInt8))))))
FORMAT Null;

SELECT throwIf(NOT isNull(res))
FROM
(
    SELECT arrayReverse(a) AS res
    FROM values('id UInt8, a Nullable(Array(UInt8))', (1, NULL), (2, [1, 2]))
    WHERE id = 1
)
FORMAT Null;

SELECT throwIf(res != [2, 1])
FROM
(
    SELECT arrayReverse(a) AS res
    FROM values('id UInt8, a Nullable(Array(UInt8))', (1, NULL), (2, [1, 2]))
    WHERE id = 2
)
FORMAT Null;

SELECT throwIf(NOT isNull(res))
FROM
(
    SELECT arrayDistinct(a) AS res
    FROM values('id UInt8, a Nullable(Array(UInt8))', (1, NULL), (2, [1, 2, 1]))
    WHERE id = 1
)
FORMAT Null;

SELECT throwIf(res != [1, 2])
FROM
(
    SELECT arrayDistinct(a) AS res
    FROM values('id UInt8, a Nullable(Array(UInt8))', (1, NULL), (2, [1, 2, 1]))
    WHERE id = 2
)
FORMAT Null;

SELECT 'ok';
