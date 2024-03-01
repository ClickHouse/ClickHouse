SET allow_suspicious_low_cardinality_types=1;

SELECT arrayFold((acc, x) -> (acc + (x * 2)), [1, 2, 3, 4], toInt64(3));
SELECT arrayFold((acc, x) -> (acc + (x * 2)), [1, 2, 3, 4], toInt64(toNullable(3)));
SELECT arrayFold((acc, x) -> (acc + (x * 2)), [1, 2, 3, 4], materialize(toInt64(toNullable(3))));

SELECT arrayFold((acc, x) -> (acc + (x * 2)), [1, 2, 3, 4]::Array(Nullable(Int64)), toInt64(3)); -- { serverError TYPE_MISMATCH }
SELECT arrayFold((acc, x) -> (acc + (x * 2)), [1, 2, 3, 4]::Array(Nullable(Int64)), toInt64(toNullable(3)));

SELECT arrayFold((acc, x) -> (acc + (x * 2)), []::Array(Int64), toInt64(3));
SELECT arrayFold((acc, x) -> (acc + (x * 2)), []::Array(Nullable(Int64)), toInt64(toNullable(3)));
SELECT arrayFold((acc, x) -> (acc + (x * 2)), []::Array(Nullable(Int64)), toInt64(NULL));

SELECT arrayFold((acc, x) -> x, materialize(CAST('[0, 1]', 'Array(Nullable(UInt8))')), toUInt8(toNullable(0)));
SELECT arrayFold((acc, x) -> x, materialize(CAST([NULL], 'Array(Nullable(UInt8))')), toUInt8(toNullable(0)));
SELECT arrayFold((acc, x) -> acc + x, materialize(CAST([NULL], 'Array(Nullable(UInt8))')), toUInt64(toNullable(0)));
SELECT arrayFold((acc, x) -> acc + x, materialize(CAST([1, 2, NULL], 'Array(Nullable(UInt8))')), toUInt64(toNullable(0)));

SELECT arrayFold((acc, x) -> toNullable(acc + (x * 2)), [1, 2, 3, 4], toInt64(3)); -- { serverError TYPE_MISMATCH }
SELECT arrayFold((acc, x) -> toNullable(acc + (x * 2)), [1, 2, 3, 4], toNullable(toInt64(3)));

SELECT arrayFold((acc, x) -> (acc + (x * 2)), [1, 2, 3, 4], toLowCardinality(toInt64(3))); -- { serverError TYPE_MISMATCH }
SELECT arrayFold((acc, x) -> toLowCardinality(acc + (x * 2)), [1, 2, 3, 4], toLowCardinality(toInt64(3)));
SELECT arrayFold((acc, x) -> (acc + (x * 2)), [1, 2, 3, 4]::Array(LowCardinality(Int64)), toInt64(toLowCardinality(3))); -- { serverError TYPE_MISMATCH }
SELECT arrayFold((acc, x) -> toLowCardinality(acc + (x * 2)), [1, 2, 3, 4]::Array(LowCardinality(Int64)), toInt64(toLowCardinality(3)));

SELECT arrayFold((acc, x) -> acc + (x * 2), [1, 2, 3, 4]::Array(Nullable(Int64)), toInt64(toLowCardinality(3))); -- { serverError TYPE_MISMATCH }
SELECT arrayFold((acc, x) -> toLowCardinality(acc + (x * 2)), [1, 2, 3, 4]::Array(Nullable(Int64)), toInt64(toLowCardinality(3))); -- { serverError TYPE_MISMATCH }
SELECT arrayFold((acc, x) -> toLowCardinality(acc + (x * 2)), [1, 2, 3, 4]::Array(Nullable(Int64)), toInt64(toNullable(3))); -- { serverError TYPE_MISMATCH }

SELECT arrayFold((acc, x) -> (acc + (x * 2)), [1, 2, 3, 4], NULL);
-- It's debatable which one of the following 2 queries should work, but considering the return type must match the
-- accumulator type it makes sense to be the second one
SELECT arrayFold((acc, x) -> (acc + (x * 2)), [1, 2, 3, 4], NULL::LowCardinality(Nullable(Int64))); -- { serverError TYPE_MISMATCH }
SELECT arrayFold((acc, x) -> (acc + (x * 2))::LowCardinality(Nullable(Int64)), [1, 2, 3, 4], NULL::LowCardinality(Nullable(Int64)));
