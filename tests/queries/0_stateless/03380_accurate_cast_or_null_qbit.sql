-- Regression test: https://github.com/ClickHouse/ClickHouse/issues/100158

SELECT accurateCastOrNull([1]::SimpleAggregateFunction(any, Array(Int)), 'QBit(Float32, 1)');
SELECT accurateCastOrNull([1, 2, 3]::Array(Int32), 'QBit(Float32, 3)');
SELECT accurateCastOrNull(42::UInt32, 'QBit(Float32, 1)');

-- Regression test: https://github.com/ClickHouse/ClickHouse/issues/107951 (site 13)
-- accurateCastOrNull from Array(Dynamic)/Array(Variant) to QBit used to abort: the per-variant
-- element conversion returns Nullable while the QBit element column is non-nullable.
SELECT accurateCastOrNull(arrayMap(x -> x % 3, range(8))::Array(Dynamic), 'QBit(Float32, 8)') SETTINGS allow_experimental_qbit_type = 1;
SELECT accurateCastOrNull(arrayMap(x -> toInt64(x % 3)::Variant(Int64, String), range(8))::Array(Variant(Int64, String)), 'QBit(Float32, 8)') SETTINGS allow_experimental_qbit_type = 1, allow_experimental_variant_type = 1;
SELECT accurateCastOrNull([NULL, 9223372036854775807, 1048576]::Array(Dynamic), 'QBit(Float32, 114)') SETTINGS allow_experimental_qbit_type = 1; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

-- A correct-size array with an inaccurately-convertible element must make the whole QBit NULL
-- (QBit elements are non-nullable), not silently expose the element default.
SELECT accurateCastOrNull(['abc', '1', '2']::Array(Dynamic), 'QBit(Float32, 3)') IS NULL SETTINGS allow_experimental_qbit_type = 1;
SELECT accurateCastOrNull(['abc', '1', '2']::Array(Variant(String, Int64)), 'QBit(Float32, 3)') IS NULL SETTINGS allow_experimental_qbit_type = 1, allow_experimental_variant_type = 1;
SELECT accurateCastOrNull([NULL, 1, 2]::Array(Dynamic), 'QBit(Float32, 3)') IS NULL SETTINGS allow_experimental_qbit_type = 1;
SELECT accurateCastOrNull([9223372036854775807, 1, 2]::Array(Dynamic), 'QBit(Float32, 3)') IS NULL SETTINGS allow_experimental_qbit_type = 1;
-- A fully convertible array stays non-NULL.
SELECT accurateCastOrNull([1, 2, 3]::Array(Dynamic), 'QBit(Float32, 3)') IS NULL SETTINGS allow_experimental_qbit_type = 1;
-- Per-row: only the row with the inaccurate element is NULL.
SELECT n, accurateCastOrNull(a, 'QBit(Float32, 3)') IS NULL FROM (SELECT 1 AS n, [1, 2, 3]::Array(Dynamic) AS a UNION ALL SELECT 2, ['x', '1', '2'] UNION ALL SELECT 3, [4, 5, 6]) ORDER BY n SETTINGS allow_experimental_qbit_type = 1;
