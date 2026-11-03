-- Nullable defaults for Array(T) keep the legacy denulling behavior.
SELECT arrayShiftRight([1, 2, 3], 1, toNullable(9));
SELECT arrayShiftLeft([1, 2, 3], 1, toNullable(9));
SELECT arrayShiftRight([1::Int64, 2, 3], 1, NULL::Nullable(Int64));
SELECT arrayShiftLeft([1::Int64, 2, 3], 1, NULL::Nullable(Int64));

-- NULL rows in a nullable default column are normalized to the nested default.
SELECT arrayShiftRight([1::Int64, 2, 3], 1, if(number = 0, 9::Nullable(Int64), NULL::Nullable(Int64)))
FROM numbers(2)
ORDER BY number;
SELECT arrayShiftLeft([1::Int64, 2, 3], 1, if(number = 0, 9::Nullable(Int64), NULL::Nullable(Int64)))
FROM numbers(2)
ORDER BY number;
