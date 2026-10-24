SET sql_compatible_division_for_integers = 0;
SELECT toTypeName(7 / 2), divide(7, 2), isInfinite(1 / 0), isNaN(0 / 0);

SET sql_compatible_division_for_integers = 1;
SELECT 13 / 15 * 7, divide(7, 2), -7 / 2, 7 / -2, -7 / -2;
SELECT
    toTypeName(toUInt8(7) / toUInt256(2)),
    toTypeName(toUInt16(7) / toInt8(2)),
    toTypeName(toUInt32(7) / toUInt8(2)),
    toTypeName(toUInt64(7) / toInt128(2)),
    toTypeName(toUInt128(7) / toUInt8(2)),
    toTypeName(toUInt256(7) / toUInt8(2));
SELECT
    toTypeName(toInt8(7) / toUInt256(2)),
    toTypeName(toInt16(7) / toUInt8(2)),
    toTypeName(toInt32(7) / toUInt8(2)),
    toTypeName(toInt64(7) / toUInt8(2)),
    toTypeName(toInt128(7) / toUInt8(2)),
    toTypeName(toInt256(7) / toUInt8(2));

-- Exercise constant/column, column/constant and column/column execution.
SELECT number / 3, 7 / (number + 1), (toInt64(number) - 4) / materialize(toUInt64(3)) FROM numbers(7);
SELECT toUInt8(200) / toInt8(2), materialize(toUInt8(200)) / toInt8(2), toUInt8(200) / materialize(toInt8(2));
SELECT toInt8(-128) / toUInt8(255), materialize(toInt8(-128)) / materialize(toUInt8(255));
SELECT toUInt64('18446744073709551615') / toInt64(3), materialize(toUInt64('18446744073709551615')) / materialize(toInt64(3));
SELECT toUInt128('340282366920938463463374607431768211455') / toInt8(3);
SELECT toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935') / toUInt8(1);
SELECT materialize(toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935')) / materialize(toInt8(3));
SELECT toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968') / toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935');
SELECT toInt8(-128) / toInt16(1), toInt8(-128) / toInt16(256), toInt8(127) / toInt16(-1);

SELECT NULL / 0, 1 / NULL, CAST(NULL, 'Nullable(Int64)') / 0, toTypeName(toNullable(7) / 2);
SELECT a / b FROM values('a Nullable(Int8), b Nullable(Int8)', (NULL, 0), (-128, NULL), (NULL, -1), (7, 2), (-7, 2), (7, NULL));
SELECT nullIf(materialize(toInt8(-128)), materialize(toInt8(-128))) / materialize(toInt8(-1));
SELECT divide(materialize(toInt8(1)), nullIf(materialize(toInt8(0)), materialize(toInt8(0))));
SELECT toLowCardinality(toNullable(number + 1)) / toInt8(2) FROM numbers(4);
SELECT toInt8(7) / toLowCardinality(toNullable(number + 1)) FROM numbers(4);
SELECT a / toLowCardinality(b) FROM values('a Nullable(Int8), b Nullable(Int8)', (NULL, 0), (-128, NULL), (7, 2));
SELECT number / materialize(toUInt64(0)) FROM numbers(0);
SELECT arrayMap(x -> x / 2, [toInt64(-7), 0, 7]);
SELECT if(number = 0, 0, 7 / number) FROM numbers(3) SETTINGS short_circuit_function_evaluation = 'enable';

SELECT toDecimal32('7.5', 1) / 2, 7 / toDecimal32('2.0', 1), toDecimal64('7.50', 2) / toDecimal32('2.0', 1);
SELECT 7.0 / 2, 7 / 2.0, toTypeName(7 / 2.0), divideOrNull(7, 2), intDiv(7, 2);
SET sql_compatible_division_for_integers = 0;
SELECT 7 / 2, toTypeName(7 / 2) SETTINGS sql_compatible_division_for_integers = 1;
SELECT 7 / 2, toTypeName(7 / 2);
