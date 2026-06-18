-- Tags: no-random-settings

SET allow_experimental_nullable_array_type = 1;
SET short_circuit_function_evaluation_for_nulls = 0;

SELECT id, isNull(res), ifNull(res, 0)
FROM
(
    SELECT id, arrayUniq(a, b) AS res
    FROM values('id UInt8, a Nullable(Array(Int32)), b Nullable(Array(Int32))',
        (1, NULL, [1]),
        (2, [3], [4]))
)
ORDER BY id;
