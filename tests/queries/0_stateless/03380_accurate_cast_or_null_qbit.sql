-- Regression test: https://github.com/ClickHouse/ClickHouse/issues/100158

SELECT accurateCastOrNull([1]::SimpleAggregateFunction(any, Array(Int)), 'QBit(Float32, 1)');
SELECT accurateCastOrNull([1, 2, 3]::Array(Int32), 'QBit(Float32, 3)');
SELECT accurateCastOrNull(42::UInt32, 'QBit(Float32, 1)');
