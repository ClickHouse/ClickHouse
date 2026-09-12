-- `modulo` took the remainder in the operand types, and the usual arithmetic conversions make that
-- unsigned as soon as the unsigned operand is at least as wide as the signed one, so a negative
-- operand wrapped to a large positive value first. `intDiv` on the same operands was already correct,
-- which is the control below.

SELECT '-- signed dividend, unsigned divisor of equal or greater width';
SELECT modulo(toInt32(-1), toUInt32(10)), positiveModulo(toInt32(-1), toUInt32(10));
SELECT modulo(toInt64(-1), toUInt64(10)), positiveModulo(toInt64(-1), toUInt64(10));
SELECT modulo(toInt32(-100000), toUInt32(100000));
SELECT modulo(toInt64(-9223372036854775808), toUInt64(10));

SELECT '-- unsigned dividend, signed divisor';
SELECT modulo(toUInt32(7), toInt32(-3)), modulo(toUInt64(7), toInt64(-3));

SELECT '-- the wide integers take a separate branch';
SELECT modulo(toInt128(-1), toUInt128(10)), modulo(toInt256(-1), toUInt256(10));

SELECT '-- the other four functions share ModuloImpl';
SELECT moduloLegacy(toInt32(-1), toUInt32(10)), moduloOrZero(toInt32(-1), toUInt32(10)), moduloOrNull(toInt32(-1), toUInt32(10));
SELECT moduloOrZero(toInt64(-100000), toUInt64(100000));

SELECT '-- a narrower unsigned divisor was always correct, because promotion lifts both to signed int';
SELECT positiveModulo(toInt32(-1), toUInt16(10)), positiveModulo(toInt32(-1), 10);

SELECT '-- control: intDiv on the same operand types, unchanged';
SELECT intDiv(toInt32(-1), toUInt32(10)), intDiv(toUInt32(7), toInt32(-3)), intDiv(toInt128(-1), toUInt128(10));

SELECT '-- control: same-sign operands, unchanged';
SELECT modulo(toInt32(-7), toInt32(3)), modulo(toUInt32(7), toUInt32(3)), modulo(-7, 3), modulo(7.5, 2);

SELECT '-- control: the zero-divisor guards still fire';
SELECT moduloOrZero(toInt32(-1), toUInt32(0)), moduloOrNull(toInt32(-1), toUInt32(0));
SELECT modulo(toInt32(-1), toUInt32(0)); -- { serverError ILLEGAL_DIVISION }

SELECT '-- control: gcd and lcm were never affected';
SELECT gcd(toInt32(-4), toUInt32(6)), lcm(toInt32(-4), toUInt32(6));

SELECT '-- control: the minimal-signed-number guard fires on the same inputs as before';
SELECT modulo(toInt32(-2147483648), toInt32(-1)); -- { serverError ILLEGAL_DIVISION }
SELECT modulo(toInt32(-2147483648), toInt64(-1)); -- { serverError ILLEGAL_DIVISION }
SELECT modulo(toInt64(-9223372036854775808), toInt32(-1)); -- { serverError ILLEGAL_DIVISION }
SELECT positiveModulo(toInt32(-2147483648), toInt32(-1)); -- { serverError ILLEGAL_DIVISION }
SELECT modulo(toInt32(-2147483648), toUInt64(1)), modulo(toInt128(-1), toInt128(-1));

SELECT '-- regression: a same-width unsigned operand near the signed maximum overflowed the first fix';
SELECT modulo(toUInt16(37528), toInt32(167682982));
SELECT modulo(toInt64(-9000000000000000000), toUInt64(10000000000000000000));
SELECT positiveModulo(toInt64(-1), toUInt64(18446744073709551615));
SELECT positiveModulo(toInt8(-1), toUInt64(10000000000000000000));

SELECT '-- regression: the wide-pair pruning in FunctionBinaryArithmetic.h had the same overflow';
SELECT modulo(toUInt128(1000), toInt8(-7));
SELECT modulo(toInt8(-100), toUInt128(7));
SELECT modulo(toInt256(-100), toInt8(-7));
