-- A one-digit dividend must still be loaded by the decimal division helper.

SELECT divideDecimal(toDecimal256(5, 0), toDecimal256(1, 0), 0);
SELECT divideDecimal(toDecimal256(-9, 0), toDecimal256(3, 0));
SELECT divideDecimal(toDecimal32(0.5, 1), toDecimal32(1, 0));
SELECT divideDecimal(toDecimal32(1.5, 1), toDecimal32(1, 0), 0);
