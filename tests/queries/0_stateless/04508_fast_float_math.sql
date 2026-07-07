-- Vectorized (SIMD) implementations of exp2/exp10/log2/log10/pow behind the `fast_float_math`
-- setting. The default (precise) path must be bit-for-bit unchanged; the fast path must stay
-- correct across the whole domain (zero, negatives, NaN/Inf, subnormals, large magnitudes).

-- Default path: precise scalar results, exact on the documented integer cases.
SET fast_float_math = 0;
SELECT exp2(3) = 8 AND exp10(2) = 100 AND log2(8) = 3 AND log10(100) = 2 AND pow(2, 10) = 1024;

SET fast_float_math = 1;

-- exp2: exact on integer inputs, correct special values, and consistent with pow(2, x).
SELECT exp2(3) = 8 AND exp2(10) = 1024 AND exp2(0) = 1 AND exp2(-1) = 0.5 AND exp2(-3) = 0.125;
SELECT exp2(-inf) = 0 AND exp2(inf) = inf AND isNaN(exp2(nan));
SELECT sum(exp2(number) = pow(2, number)) = count() FROM numbers(64);
-- Integer and Float32 inputs also take the fast path.
SELECT exp2(toUInt32(5)) = 32 AND exp2(toFloat32(3)) = 8;

-- log2: exact on powers of two, correct special values.
SELECT log2(1) = 0 AND log2(2) = 1 AND log2(4) = 2 AND log2(1024) = 10 AND log2(0.5) = -1;
SELECT log2(0) = -inf AND isNaN(log2(-1)) AND log2(inf) = inf AND isNaN(log2(nan));

-- exp10 / log10: accurate to ~1e-9 relative, correct special values.
SELECT abs(exp10(0) - 1) < 1e-9 AND abs(exp10(2) - 100) / 100 < 1e-9 AND abs(exp10(-3) - 0.001) / 0.001 < 1e-9;
SELECT exp10(-inf) = 0 AND exp10(inf) = inf AND isNaN(exp10(nan));
SELECT log10(1) = 0 AND abs(log10(100) - 2) / 2 < 1e-9 AND abs(log10(1000) - 3) / 3 < 1e-9;
SELECT log10(0) = -inf AND isNaN(log10(-5)) AND log10(inf) = inf;

-- pow, constant integer exponent (including 0, negatives): exact via multiplication.
SELECT pow(2, 3) = 8 AND pow(2, 10) = 1024 AND pow(5, 2) = 25 AND pow(-2, 3) = -8 AND pow(2, -1) = 0.5;
SELECT sum(pow(number, 2) = number * number) = count() FROM numbers(1000);
SELECT sum(pow(number, 0) = 1) = count() FROM numbers(1000);
-- pow special values with integer exponent.
SELECT pow(inf, 2) = inf AND isNaN(pow(nan, 3)) AND pow(0, 3) = 0 AND pow(0, -1) = inf;

-- pow, constant positive base: b^y = exp2(y * log2(b)).
SELECT abs(pow(10, 2) - 100) / 100 < 1e-9 AND abs(pow(2.0, 0.5) - sqrt(2)) / sqrt(2) < 1e-9;
SELECT pow(1, 100000) = 1 AND pow(1, nan) = 1;
-- Non-positive base with a non-integer exponent falls back to precise pow (NaN, as libm).
SELECT isNaN(pow(-2, 0.5));
