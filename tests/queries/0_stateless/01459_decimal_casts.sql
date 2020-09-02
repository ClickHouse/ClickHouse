SELECT toUInt32(number) y, toDecimal32(y, 1), toDecimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(1);
SELECT toInt32(number) y, toDecimal32(y, 1), toDecimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(1, 1);
SELECT toInt64(number) y, toDecimal32(y, 1), toDecimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(2, 1);
SELECT toUInt64(number) y, toDecimal32(y, 1), toDecimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(3, 1);
SELECT toInt128(number) y, toDecimal32(y, 1), toDecimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(4, 1);
SELECT toInt256(number) y, toDecimal32(y, 1), toDecimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(5, 1);
SELECT toUInt256(number) y, toDecimal32(y, 1), toDecimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(6, 1);
SELECT toFloat32(number) y, toDecimal32(y, 1), toDecimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(7, 1);
SELECT toFloat64(number) y, toDecimal32(y, 1), toDecimal64(y, 5), toDecimal128(y, 6), toDecimal256(y, 7) FROM numbers(8, 1);

SELECT toInt32(toDecimal32(number, 1)), toInt64(toDecimal32(number, 1)), toInt128(toDecimal32(number, 1)) FROM numbers(9, 1);
SELECT toInt32(toDecimal64(number, 2)), toInt64(toDecimal64(number, 2)), toInt128(toDecimal64(number, 2)) FROM numbers(10, 1);
SELECT toInt32(toDecimal128(number, 3)), toInt64(toDecimal128(number, 3)), toInt128(toDecimal128(number, 3)) FROM numbers(11, 1);
SELECT toFloat32(toDecimal32(number, 1)), toFloat32(toDecimal64(number, 2)), toFloat32(toDecimal128(number, 3)) FROM numbers(12, 1);
SELECT toFloat64(toDecimal32(number, 1)), toFloat64(toDecimal64(number, 2)), toFloat64(toDecimal128(number, 3)) FROM numbers(13, 1);
SELECT toInt256(toDecimal32(number, 1)), toInt256(toDecimal64(number, 2)), toInt256(toDecimal128(number, 3)) FROM numbers(14, 1);
