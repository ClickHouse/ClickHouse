SET allow_experimental_nullable_array_type = 1;

SELECT throwIf(NOT isNull(intDiv(CAST([10] AS Nullable(Array(Int32))), CAST(NULL AS Nullable(Int32))))) FORMAT Null;

SELECT
    id,
    isNull(res) AS is_null,
    ifNull(res, []) AS value
FROM
(
    SELECT id, intDiv(a, b) AS res
    FROM values(
        'id UInt8, a Nullable(Array(Int32)), b Nullable(Int32)',
        (1, [10], NULL),
        (2, [10], 2))
)
ORDER BY id;
