-- Tags: no-fasttest

SELECT divideDecimal(toDecimal32(0, 2), toDecimal128(11.123456, 6));
SELECT divideDecimal(toDecimal64(123.123, 3), toDecimal64(0, 1)); -- { serverError 153 }
SELECT multiplyDecimal(toDecimal32(0, 2), toDecimal128(11.123456, 6));
SELECT multiplyDecimal(toDecimal32(123.123, 3), toDecimal128(0, 1));

SELECT (toDecimal256(bitShiftRight(toUInt256(-1), 4), 0) - multiplyDecimal(divideDecimal(toDecimal256(bitShiftRight(toUInt256(-1), 4), 0), toDecimal32(2, 0)), toDecimal32(2, 0))) == 1;
SELECT (toDecimal256(bitShiftRight(-1 * toUInt256(-1), 4), 0) - multiplyDecimal(divideDecimal(toDecimal256(bitShiftRight(-1 * toUInt256(-1), 4), 0), toDecimal32(2, 0)), toDecimal32(2, 0))) == 0;

SELECT multiplyDecimal(toDecimal256(bitShiftRight(toUInt256(-1), 4), 0), toDecimal128(1000000000000000000, 2)); -- { serverError 407 }
SELECT divideDecimal(toDecimal256(bitShiftRight(toUInt256(-1), 4), 0), toDecimal128(1e-15, 20)); -- { serverError 407 }

SELECT divideDecimal(toDecimal128(123.76, 2), toDecimal128(11.123456, 6));
SELECT divideDecimal(toDecimal32(123.123, 3), toDecimal128(11.4, 1), 2);

SELECT multiplyDecimal(toDecimal64(123.76, 2), toDecimal128(11.123456, 6));
SELECT multiplyDecimal(toDecimal32(123.123, 3), toDecimal128(11.4, 1), 2);
