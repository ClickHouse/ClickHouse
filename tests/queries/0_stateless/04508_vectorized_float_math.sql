-- Vectorized (SIMD) implementations of sin/cos/tan and the fast path of pow(x, 2). They must stay correct across the
-- whole domain (zero, negatives, NaN/Inf, subnormals, large magnitudes).

-- pow(x, 2) is one multiplication, the correctly rounded square, for a constant and a materialized exponent alike;
-- other exponents use precise pow.
SELECT sum(pow(b, 2) = b * b AND pow(b, materialize(2)) = b * b) = count() FROM (SELECT number / 991.0 - 0.5 AS b FROM numbers(1000));
SELECT sum(pow(toFloat32(b), 2) = toFloat64(toFloat32(b)) * toFloat64(toFloat32(b))) = count() FROM (SELECT number / 7.0 AS b FROM numbers(1000));
SELECT pow(inf, 2) = inf AND isNaN(pow(nan, 2)) AND pow(-0., 2) = 0 AND pow(1e200, 2) = inf AND pow(materialize(1e-200), 2) = 0;
SELECT abs(pow(10, 2.5) - 316.22776601683796) / 316.22776601683796 < 1e-15, pow(10, -2) = 0.01, pow(10, materialize(33)) = 1e33;
-- pow over a Dynamic argument keeps returning Nullable(Float64), not Dynamic.
SELECT toTypeName(pow(2::Dynamic, 3)) = 'Nullable(Float64)';

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
-- Inputs within a few ulp of a multiple of pi/2, where the reduced argument cancels almost completely: the result
-- (tiny for sin/cos near their zeros, huge for tan near its poles) must be accurate relative to its own magnitude.
-- Reference values are correctly rounded (computed in 150-digit arithmetic); a 3-part reduction is off by up to ~1e6 ulp here.
CREATE TEMPORARY TABLE t04508_trig_cancel (x Float64, s Float64, c Float64, t Float64);
INSERT INTO t04508_trig_cancel VALUES
    (39.269908169872416, 1.0, -2.45548340466059e-16, -4072517851686419.0),
    (5830.795965062656, 7.922952148330979e-17, 1.0, 7.922952148330979e-17),
    (505574.93494587863, -1.0, 4.994268073815922e-16, -2002295401888468.8),
    (642615.9188844458, 8.859201669192259e-17, -1.0, -8.859201669192259e-17),
    (3141592.653589793, -2.231912181360871e-10, 1.0, -2.231912181360871e-10),
    (4179321.968127247, -1.0, 1.4157737632408629e-15, -706327540433359.4),
    (7666483.53042661, -1.0, -2.032239683657796e-16, 4920679425962767.0),
    (8538273.921001451, 6.079734012674402e-16, -1.0, -6.079734012674402e-16),
    (31415926.535897933, 5.620555424855643e-10, 1.0, 5.620555424855643e-10),
    (50870383.55651479, 3.2312014478936904e-15, 1.0, 3.2312014478936904e-15),
    (-75191662.39838518, 1.0, 2.7049771397049974e-13, 3696888913852.5557);
SELECT max(abs(sin(x) - s) / abs(s)) < 1e-15, max(abs(cos(x) - c) / abs(c)) < 1e-15, max(abs(tan(x) - t) / abs(t)) < 2e-15
FROM t04508_trig_cancel;
-- Same over a dense range, using the identities that hold for exact values.
SELECT max(abs(sin(x) * sin(x) + cos(x) * cos(x) - 1)) < 5e-16, max(abs(tan(x) - sin(x) / cos(x)) / greatest(abs(tan(x)), 1)) < 5e-16
FROM (SELECT (number - 50000) * 0.9973 AS x FROM numbers(100000) UNION ALL SELECT (number - 500) * 1e4 FROM numbers(1000));
-- Beyond |x| > 1e8, and for NaN/Inf, libm is used.
SELECT abs(sin(1e9) - 0.5458434494486996) < 1e-15, abs(cos(-1e9) - 0.8378871813639024) < 1e-15, abs(tan(1e300) - 1.4214488238747245) < 1e-15;
SELECT abs(sin(0)) < 1e-15 AND abs(cos(0) - 1) < 1e-15 AND abs(tan(0)) < 1e-15 AND abs(sin(pi() / 2) - 1) < 1e-15 AND abs(cos(pi()) + 1) < 1e-15 AND abs(cos(1e-300) - 1) < 1e-15;
SELECT sin(-0.), cos(-0.), tan(-0.);
SELECT isNaN(sin(inf)) AND isNaN(cos(-inf)) AND isNaN(tan(nan));
-- Integer and Float32 inputs also take the vectorized path and return Float64.
SELECT abs(sin(toUInt32(1)) - 0.8414709848078965) < 1e-15, toTypeName(cos(toFloat32(1))), abs(tan(toInt8(1)) - 1.5574077246549023) < 1e-15;
-- Float32 and Decimal inputs are promoted into a separate buffer, so the libm fallback still sees the original values.
SELECT abs(sin(toFloat32(1e9)) - 0.5458434494486996) < 1e-15, abs(cos(toDecimal64(-1e9, 0)) - 0.8378871813639024) < 1e-15, isNaN(sin(toFloat32(inf)));
