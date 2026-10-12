-- Negative zero is equal to positive zero, also when the floating point values are `LowCardinality`.

SET allow_suspicious_low_cardinality_types = 1;

SELECT transform(CAST(-0.0, 'LowCardinality(Float64)'), CAST([0.0], 'Array(LowCardinality(Float64))'), ['hit'], 'miss');
SELECT transform(CAST(0.0, 'LowCardinality(Float64)'), CAST([-0.0], 'Array(LowCardinality(Float64))'), ['hit'], 'miss');
SELECT transform(CAST(-0.0, 'LowCardinality(Float32)'), CAST([0.0], 'Array(LowCardinality(Float32))'), ['hit'], 'miss');
SELECT transform(CAST(-0.0, 'LowCardinality(Nullable(Float64))'), CAST([0.0], 'Array(LowCardinality(Nullable(Float64)))'), ['hit'], 'miss');
SELECT transform(CAST(x, 'LowCardinality(Float64)'), CAST([0.0, 1.0], 'Array(LowCardinality(Float64))'), ['zero', 'one'], 'other') FROM (SELECT arrayJoin([-0.0, 0.0, 1.0, 2.0]) AS x);

SELECT arrayDistinct(CAST([-0.0, 0.0], 'Array(LowCardinality(Float64))'));
SELECT arrayDistinct(CAST([0.0, -0.0, 1.0], 'Array(LowCardinality(Float32))'));
SELECT arrayDistinct(CAST([-0.0, NULL, 0.0], 'Array(LowCardinality(Nullable(Float64)))'));
SELECT arrayDistinct(CAST([1.0, 2.0, 1.0], 'Array(LowCardinality(Float64))'));
