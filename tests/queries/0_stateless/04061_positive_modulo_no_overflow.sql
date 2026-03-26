-- Regression test: positiveModulo should not cause signed integer overflow (UBSan)
-- The overflow happened when res (negative modulo result) + b overflowed signed range.

-- Both operands are large Int64 values that previously triggered:
-- "signed integer overflow: -X + -Y cannot be represented in type 'long'"
SELECT positiveModulo(toInt64(-6413604183809405374), toUInt64(12472884073595470295));
SELECT positiveModulo(toInt64(-1), toUInt64(18446744073709551615));

-- Signed divisor path
SELECT positiveModulo(toInt64(-6413604183809405374), toInt64(6472884073595470295));
SELECT positiveModulo(toInt64(-1), toInt64(9223372036854775807));

-- Small values sanity check
SELECT positiveModulo(toInt64(-7), toUInt64(5));
SELECT positiveModulo(toInt64(-7), toInt64(5));
SELECT positiveModulo(toInt64(-7), toInt64(-5));
