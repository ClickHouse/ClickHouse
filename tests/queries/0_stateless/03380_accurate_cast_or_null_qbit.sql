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
