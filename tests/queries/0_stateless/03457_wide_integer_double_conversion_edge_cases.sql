-- Test specific edge cases in double to wide integer conversion
-- Focus on values that could differ between x86 80-bit and ARM 128-bit long double

-- Values near Int128 boundaries from double literals - output actual values for comparison
SELECT toInt128(-1.7014118346046923e38);
SELECT toInt128(1.7014118346046923e38);
SELECT toInt128(-1.701411834604692e38);
SELECT toInt128(1.701411834604692e38);

-- Test that LCM doesn't overflow on near-boundary values - output the actual LCM result
SELECT lcm(toInt128(-1.7014118346046923e38), toInt128(-1.7014118346046923e38));

-- Conversions of large values (double precision limits exact representation)
SELECT toInt128(1.0e38);
SELECT toUInt128(2.0e38);
SELECT toInt256(1.0e76);

-- Decimal arithmetic that exercises set_multiplier remainder calculation
SELECT multiplyDecimal(toDecimal256(9.999999999999999e37, 0), toDecimal256(9.999999999999999e37, 0));

-- Values that fit exactly in double mantissa (53 bits)
SELECT toInt128(9007199254740991);    -- 2^53 - 1 (exact in double)
SELECT toInt128(9007199254740992);    -- 2^53 (exact in double)
SELECT toInt128(9007199254740993);    -- 2^53 + 1 (rounds in double)

-- Values that fit in 64-bit mantissa but not 53-bit
SELECT toInt128(18446744073709551615);  -- 2^64 - 1 (rounds in double)

-- Test precision loss boundaries
SELECT toInt128(1.2345678901234567e20);  -- Has more precision than double can represent
SELECT toInt128(1.2345678901234567e30);
SELECT toInt128(1.2345678901234567e38);

-- Verify consistent rounding for values in different magnitude ranges
SELECT toInt128(1.1e10);
SELECT toInt128(1.1e20);
SELECT toInt128(1.1e30);
SELECT toInt128(1.1e38);

-- Test negative values in same ranges
SELECT toInt128(-1.1e10);
SELECT toInt128(-1.1e20);
SELECT toInt128(-1.1e30);
SELECT toInt128(-1.1e38);

-- Fractional values that should truncate consistently
SELECT toInt128(1.5e38);
SELECT toInt128(1.9e38);
SELECT toInt128(-1.5e38);
SELECT toInt128(-1.9e38);

-- Values at powers of 2 boundaries
SELECT toInt128(1.0e127);  -- Near 2^127
SELECT toInt256(1.0e255);  -- Near 2^255

-- Verify Decimal256 conversions maintain precision
SELECT toDecimal256(1.23456789012345678901234567890e38, 10);
SELECT toDecimal256(-1.23456789012345678901234567890e38, 10);

-- Test that small fractional differences in double literals may produce same or different Int128 due to rounding
SELECT toInt128(1.7014118346046922e38);
SELECT toInt128(1.7014118346046923e38);
