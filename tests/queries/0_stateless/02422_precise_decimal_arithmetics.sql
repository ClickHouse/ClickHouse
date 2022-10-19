SELECT divideDecimal(toDecimal32(123.123, 2), toDecimal128(11.123456, 6), 0);
SELECT divideDecimal(toDecimal64(123.123, 2), toDecimal64(11.123456, 6), 10);
SELECT divideDecimal(toDecimal256(123.123, 2), toDecimal128(11.123456, 6), 5);