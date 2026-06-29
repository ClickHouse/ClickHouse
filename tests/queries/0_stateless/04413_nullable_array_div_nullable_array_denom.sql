SET allow_experimental_nullable_array_type = 1;

-- intDiv with a Nullable(Array) denominator that is NULL should return NULL, not ILLEGAL_DIVISION
SELECT throwIf(
    NOT isNull(intDiv(10, nullIf(CAST([0] AS Nullable(Array(Int32))), CAST([0] AS Nullable(Array(Int32)))))),
    'Expected NULL for nullable-array NULL denominator'
) FORMAT Null;

-- Mixed rows: NULL denominator and non-NULL denominator
SELECT isNull(res) AS is_null, ifNull(res, 0) AS val
FROM (
    SELECT intDiv(a, b) AS res
    FROM values(
        'a Nullable(Array(Int32)), b Nullable(Array(Int32))',
        ([10], [2]),
        ([10], NULL))
)
ORDER BY is_null, val;
