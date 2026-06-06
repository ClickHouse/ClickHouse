-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 1;
SET short_circuit_function_evaluation_for_nulls = 0;

SELECT throwIf(NOT isNull(res))
FROM
(
    SELECT a + b AS res
    FROM values('id UInt8, a Nullable(Array(Int32)), b Nullable(Array(Int32))', (1, NULL, [1]), (2, [3], [4]))
    WHERE id = 1
)
FORMAT Null;

SELECT throwIf(res != [7])
FROM
(
    SELECT a + b AS res
    FROM values('id UInt8, a Nullable(Array(Int32)), b Nullable(Array(Int32))', (1, NULL, [1]), (2, [3], [4]))
    WHERE id = 2
)
FORMAT Null;

SELECT throwIf(NOT isNull(res))
FROM
(
    SELECT a - b AS res
    FROM values('id UInt8, a Nullable(Array(Int32)), b Nullable(Array(Int32))', (1, NULL, [1]), (2, [3], [4]))
    WHERE id = 1
)
FORMAT Null;

SELECT throwIf(res != [-1])
FROM
(
    SELECT a - b AS res
    FROM values('id UInt8, a Nullable(Array(Int32)), b Nullable(Array(Int32))', (1, NULL, [1]), (2, [3], [4]))
    WHERE id = 2
)
FORMAT Null;

SELECT throwIf(NOT isNull(res))
FROM
(
    SELECT a * n AS res
    FROM values('id UInt8, a Nullable(Array(Int32)), n UInt8', (1, NULL, 2), (2, [3], 2))
    WHERE id = 1
)
FORMAT Null;

SELECT throwIf(res != [6])
FROM
(
    SELECT a * n AS res
    FROM values('id UInt8, a Nullable(Array(Int32)), n UInt8', (1, NULL, 2), (2, [3], 2))
    WHERE id = 2
)
FORMAT Null;
