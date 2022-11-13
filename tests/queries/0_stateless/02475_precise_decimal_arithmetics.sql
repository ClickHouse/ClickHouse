-- Tags: no-fasttest

SELECT divideDecimal(toDecimal32(0, 2), toDecimal128(11.123456, 6));
SELECT divideDecimal(toDecimal64(123.123, 3), toDecimal64(0, 1)); -- { serverError 153 }
SELECT multiplyDecimal(toDecimal32(0, 2), toDecimal128(11.123456, 6));
SELECT multiplyDecimal(toDecimal32(123.123, 3), toDecimal128(0, 1));

-- don't look at strange query result -- it happens due to bad float precision: toUInt256(1e38) == 99999999999999997752612184630461283328
SELECT multiplyDecimal(toDecimal256(1e38, 0), toDecimal256(1e38, 0));
SELECT divideDecimal(toDecimal256(1e66, 0), toDecimal256(1e-10, 10), 0);

-- fits Decimal256, but scale is too big to fit
SELECT multiplyDecimal(toDecimal256(1e38, 0), toDecimal256(1e38, 0), 2); -- { serverError 407 }
SELECT divideDecimal(toDecimal256(1e72, 0), toDecimal256(1e-5, 5), 2); -- { serverError 407 }

-- does not fit Decimal256
SELECT multiplyDecimal(toDecimal256(1e39, 0), toDecimal256(1e39, 0), 0); -- { serverError 407 }
SELECT divideDecimal(toDecimal256(1e39, 0), toDecimal256(1e-38, 39)); -- { serverError 407 }

SELECT divideDecimal(toDecimal128(123.76, 2), toDecimal128(11.123456, 6));
SELECT divideDecimal(toDecimal32(123.123, 3), toDecimal128(11.4, 1), 2);
SELECT divideDecimal(toDecimal128(-123.76, 2), toDecimal128(11.123456, 6));
SELECT divideDecimal(toDecimal32(123.123, 3), toDecimal128(-11.4, 1), 2);
SELECT divideDecimal(toDecimal32(-123.123, 3), toDecimal128(-11.4, 1), 2);

SELECT multiplyDecimal(toDecimal64(123.76, 2), toDecimal128(11.123456, 6));
SELECT multiplyDecimal(toDecimal32(123.123, 3), toDecimal128(11.4, 1), 2);
SELECT multiplyDecimal(toDecimal64(-123.76, 2), toDecimal128(11.123456, 6));
SELECT multiplyDecimal(toDecimal32(123.123, 3), toDecimal128(-11.4, 1), 2);
SELECT multiplyDecimal(toDecimal32(-123.123, 3), toDecimal128(-11.4, 1), 2);
