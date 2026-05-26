-- Regression: transform's from/to arrays must be constant; non-constant arrays must not be cached.

SELECT transform(number % 2, [0, 1], [number, number + 10]) FROM numbers(3); -- { clientError ILLEGAL_COLUMN }

SELECT transform(number % 2, [0, 1], materialize([0, 10])) FROM numbers(3); -- { clientError ILLEGAL_COLUMN }

SET allow_experimental_nullable_array_type = 1;

SELECT throwIf(transform(1, CAST([1, 2] AS Nullable(Array(UInt8))), CAST([10, 20] AS Nullable(Array(UInt8)))) != 10)
FORMAT Null;
