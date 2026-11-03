-- A decimal brought to the common scale whose product does not fit compares beyond every value of the other side,
-- and a cast to such a scale throws.

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

SELECT '-- X = 2^98 + 1, X * 10^30 wraps to 10^30';
SELECT toDecimal128(1, 30) = toDecimal128('316912650057057350374175801345', 0), materialize(toDecimal128(1, 30)) = materialize(toDecimal128('316912650057057350374175801345', 0));
SELECT tuple(toDecimal128(1, 30)) = tuple(toDecimal128('316912650057057350374175801345', 0)), toNullable(toDecimal128(1, 30)) = toDecimal128('316912650057057350374175801345', 0);
SELECT has([toDecimal128(1, 30)], toDecimal128('316912650057057350374175801345', 0)), indexOf([toDecimal128(1, 30)], toDecimal128('316912650057057350374175801345', 0));
SELECT
    has(materialize([toDecimal128(1, 30)]), materialize(toDecimal128('316912650057057350374175801345', 0))),
    indexOf(materialize([toDecimal128(1, 30)]), materialize(toDecimal128('316912650057057350374175801345', 0))),
    has(materialize([toDecimal128(1, 30)]), toDecimal128('316912650057057350374175801345', 0)),
    countEqual(materialize([toDecimal128(1, 30)]), materialize(toDecimal128('316912650057057350374175801345', 0))),
    indexOfAssumeSorted(materialize([toDecimal128(1, 30)]), materialize(toDecimal128('316912650057057350374175801345', 0))),
    mapContainsValue(materialize(map(1, toDecimal128(1, 30))), materialize(toDecimal128('316912650057057350374175801345', 0)));
SELECT
    has(materialize([toDecimal128(1, 30), NULL]), materialize(toNullable(toDecimal128('316912650057057350374175801345', 0)))),
    has(materialize([toDecimal128(1, 30), NULL]), materialize(CAST(NULL, 'Nullable(Decimal(38, 0))'))),
    indexOf(materialize([NULL, toDecimal128('1.5', 1), toDecimal128(1, 1)]), materialize(toDecimal128(1, 0))),
    indexOf(materialize([toDecimal32(2, 9), toDecimal32(1, 9)]), materialize(toDecimal64(1, 0)));
SELECT
    has(materialize([toDecimal128(1, 30)]), toNullable(toDecimal128('316912650057057350374175801345', 0))),
    has(materialize([toDecimal128(1, 30), NULL]), CAST(NULL, 'Nullable(Decimal(38, 0))')),
    has(materialize([toDecimal128(1, 30)]), CAST(NULL, 'Nullable(Decimal(38, 0))')),
    indexOf(materialize([toDecimal128('316912650057057350374175801345', 0), NULL]), CAST(NULL, 'Nullable(Decimal(38, 30))'));

SELECT '-- the primary key';
DROP TABLE IF EXISTS t_05339;
CREATE TABLE t_05339 (d Decimal(38, 0)) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_05339 VALUES (1), (100000000000000000000), ('316912650057057350374175801345');
SELECT count() FROM t_05339 WHERE d > toDecimal128(1, 30);
SELECT count() FROM t_05339 WHERE d = toDecimal128(1, 30);
DROP TABLE t_05339;
CREATE TABLE t_05339 (d Decimal(18, 0)) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_05339 VALUES (1), (922337204), (70368744177665);
SELECT count() FROM t_05339 WHERE d > toDecimal64(1, 18);
SELECT count() FROM t_05339 WHERE d = toDecimal64(1, 18);
DROP TABLE t_05339;

SELECT '-- a key scale that the constant does not fit';
CREATE TABLE t_05339 (d Decimal(38, 30)) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_05339 VALUES (-5), (1), (2), (100);
SELECT count() FROM t_05339 WHERE d = toDecimal128(1000000000, 0);
SELECT count() FROM t_05339 WHERE d != toDecimal128(1000000000, 0);
SELECT count() FROM t_05339 WHERE d IN (SELECT toDecimal128(1000000000, 0));
SELECT count() FROM t_05339 WHERE d NOT IN (SELECT toDecimal128(-1000000000, 0));
SELECT d FROM t_05339 WHERE d = toDecimal128(2, 0) SETTINGS force_primary_key = 1;
SELECT d FROM t_05339 WHERE d IN (SELECT toDecimal128(2, 0)) SETTINGS force_primary_key = 1;
SELECT count() FROM t_05339 WHERE d IN (toDecimal128(1000000000, 0));
SELECT count() FROM t_05339 WHERE d NOT IN (toDecimal128(-1000000000, 0));
SELECT d FROM t_05339 WHERE d IN (toDecimal128(2, 0), toDecimal128(1000000000, 0)) SETTINGS force_primary_key = 1;
SELECT count() FROM t_05339 WHERE isNotDistinctFrom(d, toDecimal128(1000000000, 0));
SELECT count() FROM t_05339 WHERE isDistinctFrom(d, toDecimal128(-1000000000, 0));
DROP TABLE t_05339;
CREATE TABLE t_05339 (d Decimal(76, 70)) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_05339 VALUES (1), (2);
SELECT count() FROM t_05339 WHERE d = toDecimal256(5789605, 0);
SELECT count() FROM t_05339 WHERE d IN (SELECT toDecimal256(5789605, 0));
SELECT count() FROM t_05339 WHERE d IN (toDecimal256(5789605, 0));
DROP TABLE t_05339;
CREATE TABLE t_05339 (d Decimal(18, 10)) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_05339 VALUES (1), (2);
SELECT count() FROM t_05339 WHERE d = toDecimal64(922337204, 0);
SELECT count() FROM t_05339 WHERE d = toDecimal32(2147483647, 0);
SELECT count() FROM t_05339 WHERE d IN (SELECT toDecimal64(922337204, 0));
SELECT count() FROM t_05339 WHERE d IN (toDecimal64(922337204, 0));
SELECT count() FROM t_05339 WHERE d IN (toDecimal128('100000000000000000000', 10));
SELECT count() FROM t_05339 WHERE isNotDistinctFrom(d, toDecimal64(922337204, 0));
DROP TABLE t_05339;
SELECT tuple(CAST(NULL, 'Nullable(Decimal(38, 30))')) IN (tuple(toDecimal128(1000000000, 0))) SETTINGS transform_null_in = 1;
SELECT [CAST(NULL, 'Nullable(Decimal(38, 30))')] IN ([toDecimal128(1000000000, 0)]);

SELECT '-- Decimal64, scale 0 vs 10';
SELECT materialize(toDecimal64(922337203, 0)) = toDecimal64(922337203, 10), materialize(toDecimal64(-922337203, 0)) = toDecimal64(-922337203, 10);
SELECT materialize(toDecimal64(922337204, 0)) < toDecimal64(1, 10), materialize(toDecimal64(-922337204, 0)) < toDecimal64(1, 10);
SELECT materialize(toDecimal64(922337204, 0)) > materialize(toDecimal64(1, 10)), toDecimal64(1, 10) > materialize(toDecimal64(922337204, 0)), toDecimal64(922337204, 0) > toDecimal64(1, 10);
SELECT
    materialize(toDecimal64(922337204, 0)) > reinterpret(toInt64('9223372036854775807'), 'Decimal64(10)'),
    materialize(toDecimal64(-922337204, 0)) < reinterpret(toInt64('-9223372036854775808'), 'Decimal64(10)'),
    materialize(toDecimal64(922337204, 0)) = reinterpret(toInt64('9223372036854775807'), 'Decimal64(10)');
SELECT toDecimal64(1, 18) < 100, materialize(toDecimal64(1, 18)) < 100, 100 > materialize(toDecimal64(1, 18)), materialize(toDecimal32(1, 9)) < toDecimal64(10000000000, 0);
SELECT toDecimal64(1, 18) = toDecimal64(70368744177665, 0), materialize(toDecimal64(1, 18)) = materialize(toDecimal64(70368744177665, 0));

SELECT '-- Decimal32, scale 0 vs 9';
SELECT materialize(toDecimal32(2, 0)) = toDecimal32(2, 9), materialize(toDecimal32(-2, 0)) = toDecimal32(-2, 9);
SELECT materialize(toDecimal32(3, 0)) < toDecimal32(1, 9), materialize(toDecimal32(-3, 0)) < toDecimal32(1, 9);
SELECT materialize(toDecimal32(3, 0)) > materialize(toDecimal32(1, 9)), toDecimal32(1, 9) > materialize(toDecimal32(3, 0)), toDecimal32(1, 9) < toDecimal32(100, 0);
SELECT
    materialize(toDecimal32(3, 0)) > reinterpret(toInt32(2147483647), 'Decimal32(9)'),
    materialize(toDecimal32(-3, 0)) < reinterpret(toInt32(-2147483648), 'Decimal32(9)');
SELECT toDecimal32(1, 9) = toDecimal32(8388609, 0), materialize(toDecimal32(1, 9)) = materialize(toDecimal32(8388609, 0));

SELECT '-- Decimal256, a column brought to the scale of a constant';
SELECT d, d < toDecimal256('-1.5', 1), toDecimal256('-1.5', 1) > d, d = toDecimal256('-1.5', 1), d = toDecimal256('-2.0', 1), toDecimal256('-2.0', 1) < d, d > toDecimal256('1.5', 1)
FROM (SELECT CAST(arrayJoin([-2, -1, 1, 2]) AS Decimal(76, 0)) AS d) ORDER BY d;
SELECT '-- Decimal64';
SELECT d, d < toDecimal64('-1.5', 1), toDecimal64('-1.5', 1) > d, d = toDecimal64('-1.5', 1), d = toDecimal64('-2.0', 1), toDecimal64('-2.0', 1) < d, d > toDecimal64('1.5', 1)
FROM (SELECT CAST(arrayJoin([-2, -1, 1, 2]) AS Decimal(18, 0)) AS d) ORDER BY d;
SELECT '-- Decimal32';
SELECT d, d < toDecimal32('-1.5', 1), toDecimal32('-1.5', 1) > d, d = toDecimal32('-1.5', 1), d = toDecimal32('-2.0', 1), toDecimal32('-2.0', 1) < d, d > toDecimal32('1.5', 1)
FROM (SELECT CAST(arrayJoin([-2, -1, 1, 2]) AS Decimal(9, 0)) AS d) ORDER BY d;

SELECT '-- the cast to the common type';
SELECT [toDecimal128(1, 30)] = [toDecimal128('316912650057057350374175801345', 0)]; -- { serverError DECIMAL_OVERFLOW }
SELECT [toDecimal128(1, 30)] < [toDecimal128(1000000000, 0)]; -- { serverError DECIMAL_OVERFLOW }
SELECT map(1, toDecimal128(1, 30)) = map(1, toDecimal128('316912650057057350374175801345', 0)); -- { serverError DECIMAL_OVERFLOW }
SELECT count() FROM (SELECT toDecimal128('316912650057057350374175801345', 0) AS a) AS l JOIN (SELECT toDecimal128(1, 30) AS b) AS r ON l.a = r.b; -- { serverError DECIMAL_OVERFLOW }
SELECT arrayMax([toDecimal128(1, 30), toDecimal128(1000000000, 0)]); -- { serverError DECIMAL_OVERFLOW }
SELECT has(materialize([tuple(toDecimal128(1, 30))]), materialize(tuple(toDecimal128('316912650057057350374175801345', 0)))); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal128('316912650057057350374175801345', 0) IN (SELECT toDecimal128(1, 30));
SELECT CAST(toDecimal128(170141183, 0) AS Decimal(38, 30)), CAST(materialize(toDecimal128(-170141183, 0)) AS Decimal(38, 30));
SELECT CAST(toDecimal128(170141184, 0) AS Decimal(38, 30)); -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(materialize(toDecimal128(-170141184, 0)) AS Decimal(38, 30)); -- { serverError DECIMAL_OVERFLOW }
SELECT accurateCastOrNull(materialize(toDecimal128(170141184, 0)), 'Decimal(38, 30)'), accurateCastOrNull(toDecimal128(170141184, 0), 'Decimal(38, 30)');
SELECT CAST(materialize(toDecimal256(5789604, 0)) AS Decimal(76, 70));
SELECT CAST(materialize(toDecimal256(5789605, 0)) AS Decimal(76, 70)); -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(toDecimal256(-5789605, 0) AS Decimal(76, 70)); -- { serverError DECIMAL_OVERFLOW }

SELECT '-- an integer brought to a decimal scale';
SELECT CAST(materialize(toInt64(170141183)) AS Decimal(38, 30)), CAST(toInt64(-170141183) AS Decimal(38, 30)), toDecimal128(materialize(toUInt64(170141183)), 30);
SELECT CAST(materialize(toInt64(170141184)) AS Decimal(38, 30)); -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(toInt64(-170141184) AS Decimal(38, 30)); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal128(materialize(toUInt64(170141184)), 30); -- { serverError DECIMAL_OVERFLOW }
SELECT accurateCastOrNull(materialize(toInt64(1000000000)), 'Decimal(38, 30)'), accurateCastOrNull(toInt64(1000000000), 'Decimal(38, 30)'), accurateCastOrNull(materialize(toUInt64(170141184)), 'Decimal(38, 30)');
SELECT CAST(materialize(toInt32(5789604)) AS Decimal(76, 70)), accurateCastOrNull(materialize(toInt64(-5789604)), 'Decimal(76, 70)');
SELECT CAST(materialize(toInt32(5789605)) AS Decimal(76, 70)); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal256(materialize(toInt64(-5789605)), 70); -- { serverError DECIMAL_OVERFLOW }
SELECT accurateCastOrNull(materialize(toInt64(5789605)), 'Decimal(76, 70)'), accurateCastOrNull(toInt64(-5789605), 'Decimal(76, 70)'), accurateCastOrNull(materialize(toInt128(5789605)), 'Decimal(76, 70)');

SELECT '-- an unsigned integer that does not fit the signed intermediate';
SELECT toDecimal256(materialize(toUInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967')), 0), toDecimal256(toUInt256(5), 70), toDecimal256(materialize(toUInt256(5)), 1);
SELECT toDecimal256(bitNot(toUInt256(0)), 1); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal256(materialize(bitNot(toUInt256(0))), 1); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal256(toUInt256('57896044618658097711785492504343953926634992332820282019728792003956564819968'), 0); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal256(materialize(toUInt256('57896044618658097711785492504343953926634992332820282019728792003956564819968')), 0); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal128(materialize(bitNot(toUInt256(0))), 0); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal64(bitNot(toUInt256(0)), 5); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal32(materialize(bitNot(toUInt256(0))), 1); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal256(arrayJoin([toUInt256(1), bitNot(toUInt256(0)), toUInt256(7)]), 0); -- { serverError DECIMAL_OVERFLOW }
