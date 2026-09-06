-- Regression for the `INT_MIN / -1` overflow guard misfiring on the smallest positive float.
-- `std::numeric_limits<Float>::min()` is the smallest positive normal value, not the domain
-- minimum, and `is_signed_v` is true for `Float32` / `Float64` / `BFloat16`, so `intDiv` threw
-- `ILLEGAL_DIVISION` and `intDivOrNull` returned NULL for an ordinary tiny positive dividend.
-- Dividing a float by -1 cannot overflow (IEEE negation is exact) and the quotient truncates
-- to 0. Same trap as `moduloLeadsToFPE`, see https://github.com/ClickHouse/ClickHouse/pull/101976
--
-- Each case is exercised in several constness shapes: `FunctionBinaryArithmetic` has distinct
-- executor branches per constness, and `identity` yields a `ColumnConst` the analyzer does not
-- fold away, so a regression on the constant side would be missed by vector/vector alone.

SET allow_experimental_bfloat16_type = 1;

SELECT 'Float64 smallest-positive intDiv -1 is 0, not an error';
SELECT intDiv(identity(toFloat64(2.2250738585072014e-308)), identity(toFloat64(-1)));
SELECT intDiv(materialize(toFloat64(2.2250738585072014e-308)), materialize(toFloat64(-1)));
SELECT intDiv(toFloat64(2.2250738585072014e-308), materialize(toFloat64(-1)));
SELECT intDiv(materialize(toFloat64(2.2250738585072014e-308)), toFloat64(-1));

SELECT 'Float32 smallest-positive intDiv -1 is 0, not an error';
SELECT intDiv(identity(toFloat32(1.17549435e-38)), identity(toFloat32(-1)));
SELECT intDiv(materialize(toFloat32(1.17549435e-38)), materialize(toFloat32(-1)));
SELECT intDiv(toFloat32(1.17549435e-38), materialize(toFloat32(-1)));
SELECT intDiv(materialize(toFloat32(1.17549435e-38)), toFloat32(-1));

SELECT 'Integer and mixed-width divisor spellings';
SELECT intDiv(toFloat64(2.2250738585072014e-308), -1);
SELECT intDiv(toFloat64(2.2250738585072014e-308), toInt8(-1));
SELECT intDiv(toFloat64(2.2250738585072014e-308), toInt64(-1));
SELECT intDiv(toFloat64(2.2250738585072014e-308), toInt128(-1));
SELECT intDiv(toFloat32(1.17549435e-38), toFloat64(-1));
SELECT intDiv(toFloat64(2.2250738585072014e-308), toFloat32(-1));

SELECT 'Type wrappers and non-scalar contexts';
SELECT intDiv(toNullable(toFloat64(2.2250738585072014e-308)), toFloat64(-1));
SELECT intDiv(toLowCardinality(toFloat64(2.2250738585072014e-308)), toFloat64(-1));
SELECT intDiv(toNullable(toLowCardinality(toFloat64(2.2250738585072014e-308))), toFloat64(-1));
SELECT arrayMap(x -> intDiv(x, toFloat64(-1)), [toFloat64(2.2250738585072014e-308)]);

SELECT 'intDivOrNull returns 0, not NULL';
SELECT intDivOrNull(identity(toFloat64(2.2250738585072014e-308)), identity(toFloat64(-1)));
SELECT intDivOrNull(materialize(toFloat64(2.2250738585072014e-308)), materialize(toFloat64(-1)));
SELECT intDivOrNull(identity(toFloat32(1.17549435e-38)), identity(toFloat32(-1)));
SELECT intDivOrNull(materialize(toFloat32(1.17549435e-38)), materialize(toFloat32(-1)));

SELECT 'intDivOrZero stays 0';
SELECT intDivOrZero(identity(toFloat64(2.2250738585072014e-308)), identity(toFloat64(-1)));
SELECT intDivOrZero(materialize(toFloat64(2.2250738585072014e-308)), materialize(toFloat64(-1)));

SELECT 'BFloat16 smallest-positive (bit pattern 256)';
SELECT reinterpretAsUInt16(toBFloat16(2.350988701644575e-38));
SELECT intDiv(identity(toBFloat16(2.350988701644575e-38)), identity(toBFloat16(-1)));
SELECT intDiv(materialize(toBFloat16(2.350988701644575e-38)), materialize(toBFloat16(-1)));
SELECT intDivOrNull(identity(toBFloat16(2.350988701644575e-38)), identity(toBFloat16(-1)));
SELECT intDivOrNull(materialize(toBFloat16(2.350988701644575e-38)), materialize(toBFloat16(-1)));
SELECT intDivOrZero(identity(toBFloat16(2.350988701644575e-38)), identity(toBFloat16(-1)));

SELECT 'Neighbouring float values were always fine and stay 0';
SELECT intDiv(toFloat64(4.5e-308), toFloat64(-1));
SELECT intDiv(toFloat64(4.9e-324), toFloat64(-1));
SELECT intDiv(toFloat64(-2.2250738585072014e-308), toFloat64(-1));
SELECT intDiv(toFloat64(2.2250738585072014e-308), toFloat64(-2));
SELECT intDiv(toFloat64(2.2250738585072014e-308), toFloat64(1));
SELECT intDiv(toFloat64(0), toFloat64(-1));
SELECT intDiv(toFloat64(-0.0), toFloat64(-1));
SELECT reinterpretAsUInt16(toBFloat16(4.70197740376576e-38));
SELECT intDiv(toBFloat16(4.70197740376576e-38), toBFloat16(-1));
SELECT intDiv(toDecimal32(0.000000001, 9), toDecimal32(-1, 9));

SELECT 'A minimal signed integer dividend with a float divisor is still suppressed';
SELECT intDivOrZero(toInt8(-128), toFloat64(-1));
SELECT intDivOrNull(toInt8(-128), toFloat64(-1));
SELECT intDivOrNull(toInt8(-128), toFloat32(-1));
SELECT intDivOrZero(toInt64(-9223372036854775808), toFloat64(-1));
SELECT intDivOrNull(toInt64(-9223372036854775808), toFloat64(-1));
SELECT intDivOrNull(materialize(toInt64(-9223372036854775808)), materialize(toFloat64(-1)));
SELECT intDivOrZero(toInt8(-128), toUInt8(255));

SELECT 'The real integer minimal-value overflow still throws';
SELECT intDiv(toInt8(-128), toInt8(-1)); -- { serverError ILLEGAL_DIVISION }
SELECT intDiv(toInt64(-9223372036854775808), toInt64(-1)); -- { serverError ILLEGAL_DIVISION }
SELECT intDiv(materialize(toInt64(-9223372036854775808)), materialize(toInt64(-1))); -- { serverError ILLEGAL_DIVISION }
SELECT modulo(toInt64(-9223372036854775808), toInt64(-1)); -- { serverError ILLEGAL_DIVISION }
SELECT gcd(toInt64(-9223372036854775808), toInt64(-1)); -- { serverError ILLEGAL_DIVISION }
SELECT intDiv(toInt8(-128), toFloat64(-1)); -- { serverError ILLEGAL_DIVISION }
SELECT intDiv(toInt64(-9223372036854775808), toFloat64(-1)); -- { serverError ILLEGAL_DIVISION }

SELECT 'Division by zero and out-of-range floats are unaffected';
SELECT intDiv(toFloat64(2.2250738585072014e-308), toFloat64(0)); -- { serverError ILLEGAL_DIVISION }
SELECT intDivOrZero(toFloat64(1), toFloat64(0));
SELECT intDivOrNull(toFloat64(1), toFloat64(0));
SELECT intDiv(toFloat64(nan), toFloat64(-1)); -- { serverError ILLEGAL_DIVISION }
SELECT intDiv(toFloat64(inf), toFloat64(-1)); -- { serverError ILLEGAL_DIVISION }
SELECT intDiv(toFloat64(-1.7976931348623157e308), toFloat64(-1)); -- { serverError ILLEGAL_DIVISION }

SELECT 'Floating modulo keeps its own null condition';
SELECT moduloOrNull(toFloat64(2.2250738585072014e-308), toFloat64(-1)) IS NULL;
SELECT positiveModuloOrNull(toFloat64(2.2250738585072014e-308), toFloat64(-1)) IS NULL;
SELECT modulo(toFloat64(2.2250738585072014e-308), toFloat64(-1)) = toFloat64(2.2250738585072014e-308);
SELECT moduloOrNull(toFloat32(1.5), toFloat32(0)) IS NULL;

SELECT 'Result types are unchanged';
SELECT toTypeName(intDiv(toFloat64(2.2250738585072014e-308), toFloat64(-1)));
SELECT toTypeName(intDiv(toFloat32(1.17549435e-38), toFloat32(-1)));
SELECT toTypeName(intDiv(toBFloat16(2.350988701644575e-38), toBFloat16(-1)));
SELECT toTypeName(intDivOrNull(toFloat64(2.2250738585072014e-308), toFloat64(-1)));
