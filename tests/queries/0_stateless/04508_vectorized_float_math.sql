-- Vectorized (SIMD) implementations of exp2/exp10/log2/log10/pow/sin/cos/tan. They must stay
-- correct across the whole domain (zero, negatives, NaN/Inf, subnormals, large magnitudes).

-- exp2: exact on integer inputs, correct special values, and consistent with pow(2, x).
SELECT exp2(3) = 8 AND exp2(10) = 1024 AND exp2(0) = 1 AND exp2(-1) = 0.5 AND exp2(-3) = 0.125;
SELECT exp2(-inf) = 0 AND exp2(inf) = inf AND isNaN(exp2(nan));
SELECT sum(exp2(number) = pow(2, number)) = count() FROM numbers(64);
-- Integer and Float32 inputs also take the vectorized path.
SELECT exp2(toUInt32(5)) = 32 AND exp2(toFloat32(3)) = 8;

-- log2: exact on powers of two, correct special values.
SELECT log2(1) = 0 AND log2(2) = 1 AND log2(4) = 2 AND log2(1024) = 10 AND log2(0.5) = -1;
SELECT log2(0) = -inf AND isNaN(log2(-1)) AND log2(inf) = inf AND isNaN(log2(nan));

-- exp10 / log10: accurate to ~1e-9 relative, correct special values.
SELECT abs(exp10(0) - 1) < 1e-9 AND abs(exp10(2) - 100) / 100 < 1e-9 AND abs(exp10(-3) - 0.001) / 0.001 < 1e-9;
SELECT exp10(-inf) = 0 AND exp10(inf) = inf AND isNaN(exp10(nan));
SELECT log10(1) = 0 AND abs(log10(100) - 2) / 2 < 1e-9 AND abs(log10(1000) - 3) / 3 < 1e-9;
SELECT log10(0) = -inf AND isNaN(log10(-5)) AND log10(inf) = inf;

-- pow, constant integer exponent (including 0, negatives): computed by repeated multiplication.
SELECT pow(2, 3) = 8 AND pow(2, 10) = 1024 AND pow(5, 2) = 25 AND pow(-2, 3) = -8 AND pow(2, -1) = 0.5;
SELECT sum(pow(number, 2) = number * number) = count() FROM numbers(1000);
SELECT sum(pow(number, 0) = 1) = count() FROM numbers(1000);
-- Repeated multiplication is accurate but NOT bit-identical to precise pow for nontrivial floating
-- bases. Pin the real contract - agreement with precise pow to ~1e-13 relative. The precise
-- reference comes from making the exponent a non-constant column (materialize), which routes pow
-- through the std::pow fallback.
SELECT max(abs(pow(b, 17) - pow(b, materialize(17))) / abs(pow(b, materialize(17)))) < 1e-13
FROM (SELECT number / 991.0 - 0.5 AS b FROM numbers(1000)) WHERE b != 0;
SELECT abs(pow(-0.8157093076673938, 17) - pow(-0.8157093076673938, materialize(17)))
       / abs(pow(-0.8157093076673938, materialize(17))) < 1e-13;
-- pow special values with integer exponent.
SELECT pow(inf, 2) = inf AND isNaN(pow(nan, 3)) AND pow(0, 3) = 0 AND pow(0, -1) = inf;
-- Negative integer exponent near the underflow boundary: computing x^|n| then inverting would let
-- the intermediate overflow to +Inf and collapse the reciprocal to 0, wiping out this representable
-- subnormal. The result must stay non-zero and match precise pow.
SELECT pow(65698.5552524023369, -64) > 0 AND abs(pow(65698.5552524023369, -64) / 4.74709109243818793e-309 - 1) < 1e-9;
-- Integer exponents past |n| <= 64 fall back to precise pow, so pow(x, 65) is bit-identical to it.
SELECT sum(pow(materialize(b), 65) = pow(materialize(b), materialize(65))) = count()
FROM (SELECT number / 991.0 + 0.5 AS b FROM numbers(1000));
-- An integral exponent too large for Int64 must not reach the integer path - the cast would overflow.
SELECT pow(materialize(2.0), 1e19) = inf AND pow(materialize(0.5), 1e19) = 0;
-- pow over a Dynamic argument keeps returning Nullable(Float64), not Dynamic.
SELECT toTypeName(pow(2::Dynamic, 3)) = 'Nullable(Float64)';

-- pow, constant positive base: b^y = exp2(y * log2(b)).
SELECT abs(pow(10, 2) - 100) / 100 < 1e-9 AND abs(pow(2.0, 0.5) - sqrt(2)) / sqrt(2) < 1e-9;
SELECT pow(1, 100000) = 1 AND pow(1, nan) = 1;
-- Non-positive base with a non-integer exponent falls back to precise pow (NaN, as libm).
SELECT isNaN(pow(-2, 0.5));

-- sin / cos / tan against libm reference values, up to the |x| <= 1e8 limit of the polynomial kernel.
CREATE TEMPORARY TABLE t04508_trig (x Float64, s Float64, c Float64, t Float64);
INSERT INTO t04508_trig VALUES
    (0.1, 0.09983341664682815, 0.9950041652780258, 0.10033467208545055),
    (0.5, 0.479425538604203, 0.8775825618903728, 0.5463024898437905),
    (1.0, 0.8414709848078965, 0.5403023058681398, 1.5574077246549023),
    (1.23, 0.9424888019316975, 0.3342377271245026, 2.819815734268152),
    (-2.5, -0.5984721441039565, -0.8011436155469337, 0.7470222972386603),
    (3.0, 0.1411200080598672, -0.9899924966004454, -0.1425465430742778),
    (10.0, -0.5440211108893698, -0.8390715290764524, 0.6483608274590866),
    (100.0, -0.5063656411097588, 0.8623188722876839, -0.5872139151569291),
    (-1234.5, -0.14539565052293643, -0.989373592132422, 0.14695727850342305),
    (10000.0, -0.30561438888825215, -0.9521553682590148, 0.3209711346238147),
    (123456.789, -0.9986640823432246, 0.05167253271870138, -19.32678794321585),
    (1000000.0, -0.34999350217129294, 0.9367521275331447, -0.373624453987599),
    (-9870000.0, -0.9711587439445847, -0.23843383581190106, 4.07307436311483),
    (10000000.0, 0.4205477931907825, -0.9072703861817396, -0.46353082785018906),
    (99999999.0, 0.8091447235887737, 0.5876094079305121, 1.3770111789708772),
    (-99999999.0, -0.8091447235887737, 0.5876094079305121, -1.3770111789708772);
-- Accurate to ~1 ulp (relative for large results, absolute near zero).
SELECT max(abs(sin(x) - s) / greatest(abs(s), 1)) < 3e-16, max(abs(cos(x) - c) / greatest(abs(c), 1)) < 3e-16, max(abs(tan(x) - t) / greatest(abs(t), 1)) < 6e-16
FROM t04508_trig;
-- Same over a dense range, using the identities that hold for exact values.
SELECT max(abs(sin(x) * sin(x) + cos(x) * cos(x) - 1)) < 5e-16, max(abs(tan(x) - sin(x) / cos(x)) / greatest(abs(tan(x)), 1)) < 5e-16
FROM (SELECT (number - 50000) * 0.9973 AS x FROM numbers(100000) UNION ALL SELECT (number - 500) * 1e4 FROM numbers(1000));
-- Beyond |x| > 1e8, and for NaN/Inf, libm is used.
SELECT abs(sin(1e9) - 0.5458434494486996) < 1e-15, abs(cos(-1e9) - 0.8378871813639024) < 1e-15, abs(tan(1e300) - 1.4214488238747245) < 1e-15;
SELECT sin(0) = 0 AND cos(0) = 1 AND tan(0) = 0 AND abs(sin(pi() / 2) - 1) < 1e-15 AND abs(cos(pi()) + 1) < 1e-15 AND cos(1e-300) = 1;
SELECT sin(-0.), cos(-0.), tan(-0.);
SELECT isNaN(sin(inf)) AND isNaN(cos(-inf)) AND isNaN(tan(nan));
-- Integer and Float32 inputs also take the vectorized path and return Float64.
SELECT abs(sin(toUInt32(1)) - 0.8414709848078965) < 1e-15, toTypeName(cos(toFloat32(1))), abs(tan(toInt8(1)) - 1.5574077246549023) < 1e-15;
