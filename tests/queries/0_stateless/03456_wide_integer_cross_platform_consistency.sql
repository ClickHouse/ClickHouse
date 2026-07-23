-- Test cross-platform consistency of wide integer conversions from floating point
-- This ensures ARM (128-bit quad long double) produces same results as x86 (80-bit extended)

-- Basic conversions from double to wide integers
SELECT toInt128(1e38);
SELECT toInt128(-1e38);
SELECT toUInt128(1e38);
SELECT toInt256(1e38);
SELECT toInt256(-1e38);
SELECT toUInt256(1e38);

-- Edge cases near type boundaries (but not exactly at min/max)
SELECT toInt128(1.7014118346046923e38);
SELECT toInt128(-1.7014118346046923e38);
SELECT toInt128(1.701411834604692e38);  -- Slightly less than max
SELECT toInt128(-1.701411834604692e38); -- Slightly more than min

-- Very large decimal conversions (from the fixed test)
SELECT toDecimal256(1e38, 0);
SELECT toDecimal256(-1e38, 0);

-- Multiplications that exercise the set_multiplier path
SELECT multiplyDecimal(toDecimal256(1e38, 0), toDecimal256(1e38, 0));
SELECT multiplyDecimal(toDecimal128(1e19, 0), toDecimal128(1e19, 0));

-- Division with high precision
SELECT divideDecimal(toDecimal256(1e66, 0), toDecimal256(1e-10, 10), 0);
SELECT divideDecimal(toDecimal128(123.76, 2), toDecimal128(11.123456, 6));

-- JSON extraction (tests from 00918)
SELECT JSONExtract('{"a":1234567890.12345678901234567890, "b":"test"}', 'Tuple(a Decimal(35,20), b LowCardinality(String))');
SELECT JSONExtract('{"a":"1234567890.123456789012345678901234567890", "b":"test"}', 'Tuple(a Decimal(45,30), b LowCardinality(String))');
SELECT JSONExtract('{"a":123456789012345.12}', 'a', 'Decimal(30, 4)');

-- Wide integer to wide integer operations
SELECT toInt256(toInt128(1e38)) + toInt256(toInt128(1e38));
SELECT toUInt256(toUInt128(1e38)) * toUInt256(2);

-- Conversions that should produce consistent values across platforms
SELECT toInt128('170141183460469231731687303715884105727');  -- Int128 max
SELECT toInt128('-170141183460469231731687303715884105728'); -- Int128 min
SELECT toUInt128('340282366920938463463374607431768211455'); -- UInt128 max

-- GCD/LCM with large values (but not extreme edge cases)
SELECT gcd(toInt128(1e38), toInt128(2e38));
SELECT lcm(toInt128(1e30), toInt128(2e30));
SELECT gcd(toInt128(-1e38), toInt128(1e38));

-- Conversions from slightly-off boundary values
SELECT toInt128(-170141183460469231731687303715884105720); -- Int128 min + 8
SELECT toInt128(170141183460469231731687303715884105720);  -- Int128 max - 7
SELECT lcm(toInt128(-170141183460469231731687303715884105720), toInt128(-170141183460469231731687303715884105720));

-- Test precision in decimal arithmetic
SELECT multiplyDecimal(toDecimal64(123.76, 2), toDecimal128(11.123456, 6));
SELECT divideDecimal(toDecimal32(123.123, 3), toDecimal128(11.4, 1), 2);

-- Casting operations with high precision
SELECT CAST(1.7014118346046923e38 AS Int128);
SELECT CAST(-1.7014118346046923e38 AS Int128);
SELECT CAST(3.4028236692093846e38 AS UInt128);

-- Variant and dynamic type conversions
SELECT CAST(toInt128(1e38) AS String);
SELECT CAST(toInt256(1e76) AS String);

-- Ensure small values also work correctly
SELECT toInt128(1.5);
SELECT toDecimal128(1.234567890123456789, 18);
SELECT multiplyDecimal(toDecimal64(0.001, 3), toDecimal64(0.002, 3), 6);

-- Test that values in the "normal" range get consistent rounding
SELECT toInt128(9.223372036854775807e18);  -- Near Int64 max
SELECT toInt128(-9.223372036854775808e18); -- Near Int64 min
SELECT toInt256(1.234567890123456789e20);
