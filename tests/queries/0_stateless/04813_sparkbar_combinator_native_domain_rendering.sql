-- Rendering must scale nested results in their native numeric domain.
-- Values that differ only beyond the Float64 53-bit exact range must map to different bars:
-- 2^53 and 2^53 + 1 collapse to the same Float64, but exactly 2^53 / (2^53 + 1) * 7 + 1 < 8.

SELECT 'UInt64 near 2^53';
SELECT maxSparkbar(2, 0, 1)(number, 9007199254740992 + number) FROM numbers(2);

SELECT 'Int256 near 2^200';
SELECT maxSparkbar(2, 0, 1)(number, bitShiftLeft(toInt256(1), 200) + toInt256(number)) FROM numbers(2);

SELECT 'Decimal64 near 2^53';
SELECT maxSparkbar(2, 0, 1)(number, toDecimal64(9007199254740992 + number, 0)) FROM numbers(2);

-- The widening multiplication overflows Int64 here, exercising the division-based fallback:
-- buckets hold 2^62 - 1 and 2^63 - 1, expected levels 7 * 0.5 + 1 = 4 and 8.
SELECT 'Int64 overflow fallback';
SELECT maxSparkbar(2, 0, 1)(number, toInt64(9223372036854775807) - if(number = 0, toInt64(4611686018427387904), 0)) FROM numbers(2);

-- An infinite maximum must render the infinite bucket blank and finite buckets minimal,
-- not produce an out-of-range level.
SELECT 'Float64 infinity';
SELECT maxSparkbar(3, 0, 2)(number, if(number = 1, inf, 5.)) FROM numbers(3);

-- Dispatch over every supported nested result type: values 1 and 8 must render as the
-- lowest and the highest bar for each of them.
SELECT 'all result types';
SELECT maxSparkbar(2, 0, 1)(number, toUInt8(1 + 7 * number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toUInt16(1 + 7 * number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toUInt32(1 + 7 * number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toUInt64(1 + 7 * number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toUInt128(1 + 7 * number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toUInt256(1 + 7 * number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toInt8(1 + 7 * number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toInt16(1 + 7 * number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toInt32(1 + 7 * number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toInt64(1 + 7 * number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toInt128(1 + 7 * number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toInt256(1 + 7 * number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toBFloat16(1 + 7 * number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toFloat32(1 + 7 * number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toFloat64(1 + 7 * number)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toDecimal32(1 + 7 * number, 2)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toDecimal64(1 + 7 * number, 2)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toDecimal128(1 + 7 * number, 2)) FROM numbers(2);
SELECT maxSparkbar(2, 0, 1)(number, toDecimal256(1 + 7 * number, 2)) FROM numbers(2);
