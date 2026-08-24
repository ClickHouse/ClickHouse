-- Mixed-type pairs involving Decimal or 128/256-bit integers are executed via a conversion
-- to a common type instead of a dedicated fused kernel (comparisons, floating division, and
-- the integer division and modulo families; `plus`, `minus` and `multiply` keep their fused
-- kernels because they are memory-bound and the conversion would slow them down). This test
-- pins down the semantics of the affected pairs: values, result types, and exceptional cases.

SELECT '-- decimal vs integer comparisons (mixed pairs, non-constant columns)';
SELECT materialize(toDecimal32('1.50', 2)) = materialize(toUInt8(1)), materialize(toDecimal32('1.00', 2)) = materialize(toUInt8(1));
SELECT materialize(toDecimal64('-7.7777', 4)) < materialize(toInt8(-7)), materialize(toInt8(-8)) < materialize(toDecimal64('-7.7777', 4));
SELECT materialize(toDecimal128('123456789012345.5', 10)) > materialize(toUInt64(123456789012345));
SELECT materialize(toDecimal256('-1.5', 20)) != materialize(toInt256(-1));
SELECT materialize(toUInt64(123456)) > materialize(toDecimal32('1.5', 2));
SELECT materialize(CAST('18446744073709551615', 'UInt64')) > materialize(toDecimal32('1.5', 2)); -- { serverError DECIMAL_OVERFLOW }
SELECT materialize(CAST('-170141183460469231731687303715884105728', 'Int128')) < materialize(toDecimal64('0.0001', 4));

SELECT '-- decimal vs decimal comparisons of different widths and scales';
SELECT materialize(toDecimal32('1.50', 2)) = materialize(toDecimal64('1.5000', 4));
SELECT materialize(toDecimal32('1.50', 2)) = materialize(toDecimal256('1.500000', 6));
SELECT materialize(toDecimal64('-99.999', 3)) < materialize(toDecimal128('-99.9989', 4));
SELECT materialize(toDecimal128('0.1', 1)) = materialize(toDecimal256('0.10000', 5));

SELECT '-- DateTime64 vs other datetime-like types';
SELECT materialize(toDateTime64('2024-01-02 03:04:05.678', 3, 'UTC')) > materialize(toDateTime('2024-01-02 03:04:05', 'UTC'));
SELECT materialize(toDateTime64('2024-01-02 00:00:00.000', 3, 'UTC')) = materialize(toDate('2024-01-02'));
SELECT materialize(toDateTime64('2024-01-02 03:04:05.678', 3, 'UTC')) = materialize(toDateTime64('2024-01-02 03:04:05.678000', 6, 'UTC'));

SELECT '-- comparison with constants on either side';
SELECT materialize(toDecimal64('3.14', 2)) = toUInt8(3), toUInt8(3) < materialize(toDecimal64('3.14', 2));
SELECT toDecimal32('2.5', 1) >= materialize(toInt64(2)), toDecimal32('2.5', 1) <= materialize(toInt64(2));

SELECT '-- decimal comparison overflow behavior';
SELECT materialize(toDecimal256('100000000000000000000000000000000000000000', 30)) = materialize(toDecimal32('1.5', 2));
SELECT materialize(toDecimal32('9.99', 2)) = materialize(toDecimal256('0.1', 76));
SELECT materialize(toDecimal32('9.99', 2)) = materialize(toDecimal256('0.1', 76)) SETTINGS decimal_check_overflow = 0;
SELECT materialize(toDecimal32('1.5', 2)) = materialize(CAST('115792089237316195423570985008687907853269984665640564039457584007913129639935', 'UInt256')); -- { serverError DECIMAL_OVERFLOW }

SELECT '-- isNotDistinctFrom over mixed decimal pairs';
SELECT isNotDistinctFrom(materialize(toDecimal32('7.00', 2)), materialize(toInt64(7)));
SELECT isNotDistinctFrom(materialize(toDecimal64('7.5', 1)), materialize(toDecimal256('7.50', 2)));

SELECT '-- wide integer arithmetic: values and result types';
SELECT materialize(toUInt256(5)) + materialize(toUInt8(3)) AS x, toTypeName(x);
SELECT materialize(toInt256(-5)) - materialize(toUInt64(3)) AS x, toTypeName(x);
SELECT materialize(CAST('340282366920938463463374607431768211455', 'UInt128')) * materialize(toUInt8(1)) AS x, toTypeName(x);
SELECT materialize(CAST('-170141183460469231731687303715884105728', 'Int128')) + materialize(toInt8(-1)) AS x, toTypeName(x);
SELECT materialize(toUInt8(200)) + materialize(toUInt256(55)) AS x, toTypeName(x);
SELECT materialize(toUInt256(100)) / materialize(toUInt8(8)) AS x, toTypeName(x);
SELECT materialize(toInt8(-100)) / materialize(toInt256(8)) AS x, toTypeName(x);

SELECT '-- wide integer division and modulo: values, result types, sign quirks';
SELECT intDiv(materialize(toUInt256(100)), materialize(toUInt8(7))) AS x, toTypeName(x);
SELECT intDiv(materialize(toInt256(-100)), materialize(toUInt8(7))) AS x, toTypeName(x);
SELECT intDiv(materialize(toUInt256(100)), materialize(toInt8(-7))) AS x, toTypeName(x);
SELECT intDiv(materialize(toUInt8(100)), materialize(toUInt256(7))) AS x, toTypeName(x);
SELECT intDiv(materialize(toUInt128(100)), materialize(toInt128(-7))) AS x, toTypeName(x);
SELECT modulo(materialize(toUInt256(100)), materialize(toUInt8(7))) AS x, toTypeName(x);
SELECT modulo(materialize(toInt256(-100)), materialize(toUInt8(7))) AS x, toTypeName(x);
SELECT modulo(materialize(toInt256(-100)), materialize(toInt8(-7))) AS x, toTypeName(x);
SELECT modulo(materialize(toUInt8(100)), materialize(toUInt256(7))) AS x, toTypeName(x);
SELECT modulo(materialize(toInt8(-100)), materialize(toUInt256(7))) AS x, toTypeName(x);
SELECT moduloLegacy(materialize(toUInt256(1000)), materialize(toUInt8(7))) AS x, toTypeName(x);
SELECT positiveModulo(materialize(toInt256(-100)), materialize(toUInt8(7))) AS x, toTypeName(x);

SELECT '-- division exceptional cases must be preserved';
SELECT intDiv(materialize(toUInt256(100)), materialize(toUInt8(0))); -- { serverError ILLEGAL_DIVISION }
SELECT modulo(materialize(toInt256(-100)), materialize(toUInt64(0))); -- { serverError ILLEGAL_DIVISION }
SELECT intDiv(materialize(CAST('-57896044618658097711785492504343953926634992332820282019728792003956564819968', 'Int256')), materialize(toInt8(-1))); -- { serverError ILLEGAL_DIVISION }
SELECT modulo(materialize(CAST('-170141183460469231731687303715884105728', 'Int128')), materialize(toInt64(-1))); -- { serverError ILLEGAL_DIVISION }

SELECT '-- OrZero and OrNull variants over mixed wide pairs';
SELECT intDivOrZero(materialize(toUInt256(100)), materialize(toUInt8(0)));
SELECT intDivOrNull(materialize(toInt256(-100)), materialize(toUInt64(0)));
SELECT moduloOrNull(materialize(toUInt128(100)), materialize(toUInt8(0)));
SELECT moduloOrNull(materialize(toInt256(-100)), materialize(toInt8(7)));
SELECT positiveModuloOrNull(materialize(toInt128(-5)), materialize(toUInt32(3)));
SELECT intDivOrNull(materialize(CAST('-170141183460469231731687303715884105728', 'Int128')), materialize(toInt8(-1)));

SELECT '-- unsigned wrap quirks of the compute type are preserved';
SELECT modulo(materialize(toUInt256(100)), materialize(toInt8(-3))) AS x, toTypeName(x);
SELECT intDiv(materialize(CAST('115792089237316195423570985008687907853269984665640564039457584007913129639935', 'UInt256')), materialize(toUInt8(1))) AS x, toTypeName(x);
