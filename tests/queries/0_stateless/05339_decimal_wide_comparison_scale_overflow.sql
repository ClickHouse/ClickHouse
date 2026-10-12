-- A Decimal128 / Decimal256 side brought to the common scale whose product does not fit compares beyond every value of the other side.

SELECT '-- Decimal128, scale 0 vs 30';
SELECT materialize(toDecimal128(170141183, 0)) = toDecimal128(170141183, 30), materialize(toDecimal128(-170141183, 0)) = toDecimal128(-170141183, 30);
SELECT materialize(toDecimal128(170141184, 0)) < toDecimal128(1, 30), materialize(toDecimal128(-170141184, 0)) < toDecimal128(1, 30);
SELECT materialize(toDecimal128(170141184, 0)) > materialize(toDecimal128(1, 30)), toDecimal128(1, 30) > materialize(toDecimal128(170141184, 0)), toDecimal128(170141184, 0) > toDecimal128(1, 30);
SELECT materialize(toDecimal128('100000000000000000000', 0)) < toDecimal128(1, 30);
SELECT materialize(toDecimal128(170141183, 0)) = materialize(toDecimal128(170141183, 30)), materialize(toDecimal128(-170141183, 0)) = materialize(toDecimal128(-170141183, 30)), toDecimal128(170141183, 0) = materialize(toDecimal128(170141183, 30));

SELECT '-- against the stored limits of Decimal128(30)';
SELECT
    materialize(toDecimal128(170141184, 0)) > reinterpret(toInt128('170141183460469231731687303715884105727'), 'Decimal128(30)'),
    reinterpret(toInt128('170141183460469231731687303715884105727'), 'Decimal128(30)') < materialize(toDecimal128(170141184, 0)),
    materialize(toDecimal128(-170141184, 0)) < reinterpret(toInt128('-170141183460469231731687303715884105728'), 'Decimal128(30)'),
    materialize(toDecimal128(170141184, 0)) = reinterpret(toInt128('170141183460469231731687303715884105727'), 'Decimal128(30)');

SELECT '-- Decimal256, scale 0 vs 70';
SELECT materialize(toDecimal256(5789604, 0)) = toDecimal256(5789604, 70);
SELECT materialize(toDecimal256(5789605, 0)) < toDecimal256(1, 70), materialize(toDecimal256(-5789605, 0)) < toDecimal256(1, 70);
SELECT
    materialize(toDecimal256(5789605, 0)) > reinterpret(toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967'), 'Decimal256(70)'),
    materialize(toDecimal256(-5789605, 0)) < reinterpret(toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968'), 'Decimal256(70)');

SELECT '-- mixed types';
SELECT materialize(toDecimal128(1, 30)) < 170141184, materialize(toDecimal128(1, 30)) < 1000000000, toDecimal128(1, 30) < 170141184, 170141184 > materialize(toDecimal128(1, 30));
SELECT materialize(toDecimal64(170141184, 0)) < toDecimal128(1, 30);
SELECT materialize(toDecimal128(5789605, 0)) < toDecimal256(1, 70), materialize(toInt256(5789605)) > toDecimal256(1, 70);
SELECT materialize(toUInt32(170141184)) > reinterpret(toInt128('170141183460469231731687303715884105727'), 'Decimal128(30)');

SELECT '-- a column brought to the scale of a constant';
SELECT d, d < toDecimal128('-1.5', 1), toDecimal128('-1.5', 1) > d, d = toDecimal128('-1.5', 1), d = toDecimal128('-2.0', 1), toDecimal128('-2.0', 1) < d, d > toDecimal128('1.5', 1)
FROM (SELECT CAST(arrayJoin([-2, -1, 1, 2]) AS Decimal(38, 0)) AS d) ORDER BY d;
