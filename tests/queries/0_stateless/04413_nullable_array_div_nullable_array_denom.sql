SET allow_experimental_nullable_array_type = 1;

-- intDiv with a Nullable(Array) denominator that is NULL should return NULL, not ILLEGAL_DIVISION.
-- nullIf returns NULL when the two arrays are equal, so the denominator is a whole-array NULL.
SELECT throwIf(
    NOT isNull(intDiv(CAST([10] AS Nullable(Array(Int32)), nullIf(CAST([2] AS Nullable(Array(Int32))), CAST([2] AS Nullable(Array(Int32)))))),
    'Expected NULL for nullable-array NULL denominator'
) FORMAT Null;

-- intDiv with a non-NULL Nullable(Array) denominator should work normally.
SELECT throwIf(
    intDiv(CAST([10] AS Nullable(Array(Int32)), CAST([2] AS Nullable(Array(Int32)))) != [5],
    'Expected [5] for non-NULL nullable-array denominator'
) FORMAT Null;
