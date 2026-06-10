-- { echo }

SELECT '--- Basic Examples ---';
SELECT arrayAutocorrelation([1, 2, 3, 4, 5]);
SELECT arrayAutocorrelation([10, 20, 10]);

SELECT '--- Constant & Small Arrays ---';
SELECT arrayAutocorrelation([5, 5, 5]);
SELECT arrayAutocorrelation([5, 5]);
SELECT arrayAutocorrelation([5]);
SELECT arrayAutocorrelation([]);
-- Constant floats: 0.1 is not exactly representable, but all elements are bitwise identical
SELECT arrayAutocorrelation([0.1, 0.1, 0.1]);
SELECT arrayAutocorrelation(CAST([0.1, 0.1, 0.1], 'Array(Decimal64(1))'));

SELECT '--- Max Lag Argument ---';
SELECT arrayAutocorrelation([1, 2, 3, 4, 5], 2);
SELECT arrayAutocorrelation([1, 2, 3, 4, 5], 0);
SELECT arrayAutocorrelation([1, 2, 3, 4, 5], 100);
-- Extreme unsigned: UINT64_MAX, clamped to array size
SELECT arrayAutocorrelation([1, 2, 3], toUInt64('18446744073709551615'));
-- 2^63: previously caused silent wraparound via getInt
SELECT arrayAutocorrelation([1, 2, 3], toUInt64('9223372036854775808'));
-- Signed type with positive value works
SELECT arrayAutocorrelation([1, 2, 3], toInt64(2));
-- Wide integer max_lag types are rejected (only up to 64-bit)
SELECT arrayAutocorrelation([1, 2, 3], toUInt128(2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayAutocorrelation([1, 2, 3], toUInt256(2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayAutocorrelation([1, 2, 3], toInt128(2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayAutocorrelation([1, 2, 3], toInt256(2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Negative values are rejected at runtime
SELECT arrayAutocorrelation([1, 2, 3], toInt64(-1)); -- { serverError BAD_ARGUMENTS }
SELECT arrayAutocorrelation([1, 2, 3], toInt64('-9223372036854775808')); -- { serverError BAD_ARGUMENTS }

SELECT '--- Type Dispatch Coverage ---';
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(UInt8)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(UInt16)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(UInt32)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(UInt64)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(UInt128)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(UInt256)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Int8)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Int16)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Int32)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Int64)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Int128)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Int256)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Float32)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Float64)'));

SELECT '--- Decimal Support ---';
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Decimal32(2))'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Decimal64(4))'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Decimal128(6))'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Decimal256(8))'));
SELECT arrayAutocorrelation(CAST([1.5, 2.5, 3.5], 'Array(Decimal64(2))'));

SELECT '--- Table Execution ---';
DROP TABLE IF EXISTS test_autocorr;
CREATE TABLE test_autocorr (id UInt64, data Array(Float64), lag UInt64) ENGINE = Memory;
INSERT INTO test_autocorr VALUES (1, [1, 2, 3, 4, 5], 2), (2, [10, 20, 10], 5), (3, [5, 5, 5], 2);
SELECT id, arrayAutocorrelation(data) FROM test_autocorr ORDER BY id;
SELECT id, arrayAutocorrelation(data, lag) FROM test_autocorr ORDER BY id;
DROP TABLE test_autocorr;

SELECT '--- Const Array Optimization ---';
-- Non-const array with const max_lag
SELECT arrayAutocorrelation(arrayMap(x -> x + number, [1, 2, 3]), 2) FROM numbers(3);
-- Const array with non-const max_lag
SELECT arrayAutocorrelation([1, 2, 3], number) FROM numbers(3);
-- Const array with non-const signed max_lag, one row negative
SELECT arrayAutocorrelation([1, 2, 3], toInt64(number) - 1) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
-- 10-element const array across 100K rows: would OOM/TLE if materialized (10 * 100K = 1M elements)
SELECT sum(length(arrayAutocorrelation([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], number % 5))) FROM numbers(100000);
-- Same for Decimal const array
SELECT sum(length(arrayAutocorrelation(CAST([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 'Array(Decimal64(2))'), number % 5))) FROM numbers(100000);

SELECT '--- NaN and Inf Handling ---';
SELECT arrayAutocorrelation([1.0, nan, 3.0]);
SELECT arrayAutocorrelation([1.0, inf, 3.0]);
SELECT arrayAutocorrelation([nan, nan, nan]);

SELECT '--- Computation Limit ---';
SELECT length(arrayAutocorrelation(range(toUInt64(20000)), 5));
SELECT arrayAutocorrelation(range(toUInt64(20000))); -- { serverError BAD_ARGUMENTS }
-- Large constant array: variance=0 detected in O(n) before hitting computation limit
SELECT length(arrayAutocorrelation(arrayMap(x -> 5, range(toUInt64(20000)))));

SELECT '--- Nullable and LowCardinality ---';
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Nullable(UInt32))')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
-- Nullable lag: framework strips Nullable, function evaluates on the underlying integer.
SELECT arrayAutocorrelation([1, 2, 3], CAST(2, 'Nullable(UInt32)'));
SET allow_suspicious_low_cardinality_types = 1;
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(LowCardinality(UInt32))'));
SELECT arrayAutocorrelation([1, 2, 3], CAST(2, 'LowCardinality(UInt32)'));
-- LowCardinality signed lag: positive works, negative rejected
SELECT arrayAutocorrelation([1, 2, 3], CAST(2, 'LowCardinality(Int64)'));
SELECT arrayAutocorrelation([1, 2, 3], CAST(-1, 'LowCardinality(Int64)')); -- { serverError BAD_ARGUMENTS }
-- LowCardinality non-const lag
SELECT arrayAutocorrelation([1, 2, 3], CAST(number, 'LowCardinality(UInt64)')) FROM numbers(3);

SELECT '--- Negative Tests ---';
SELECT arrayAutocorrelation(['a', 'b']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayAutocorrelation([NULL, 1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(BFloat16)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayAutocorrelation(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT arrayAutocorrelation([1], 2, 3); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
