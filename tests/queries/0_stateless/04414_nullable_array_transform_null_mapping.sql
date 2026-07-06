SET allow_experimental_nullable_array_type = 1;

-- transform with a NULL Nullable(Array) mapping array should be rejected
SELECT transform(1, CAST(NULL AS Nullable(Array(UInt8))), CAST([10] AS Nullable(Array(UInt8)))); -- { serverError ILLEGAL_COLUMN }

-- transform with non-NULL Nullable(Array) mapping should work normally
SELECT throwIf(transform(1, CAST([0, 1] AS Nullable(Array(UInt8))), CAST([10, 20] AS Nullable(Array(UInt8)))) != 20, 'transform failed') FORMAT Null;
