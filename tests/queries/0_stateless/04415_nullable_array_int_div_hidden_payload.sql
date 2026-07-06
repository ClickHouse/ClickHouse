SET allow_experimental_nullable_array_type = 1;

-- intDiv with a Nullable(Array) left argument whose hidden payload would cause ILLEGAL_DIVISION
-- if the null row is not emptied before element execution.
SELECT throwIf(
    NOT isNull(intDiv(arrayPushBack(CAST(NULL AS Nullable(Array(Int32))), 1), toNullable(0))),
    'Expected NULL for nullable-array left operand with nullable denominator'
) FORMAT Null;

-- Mixed rows: one NULL array + one non-NULL array, with nullable denominator
SELECT id, isNull(res) AS is_null, ifNull(res, []) AS val
FROM (
    SELECT id, intDiv(a, b) AS res
    FROM values(
        'id UInt8, a Nullable(Array(Int32)), b Nullable(Int32)',
        (1, NULL, 0),
        (2, [10], 2))
)
ORDER BY id;
