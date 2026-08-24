-- Tags: no-fasttest

-- check cases when one of operands is zero
SELECT divideDecimal(toDecimal32(0, 2), toDecimal128(11.123456, 6));
SELECT divideDecimal(toDecimal64(123.123, 3), toDecimal64(0, 1)); -- { serverError ILLEGAL_DIVISION }
SELECT multiplyDecimal(toDecimal32(0, 2), toDecimal128(11.123456, 6));
SELECT multiplyDecimal(toDecimal32(123.123, 3), toDecimal128(0, 1));

-- don't look at strange query result -- it happens due to bad float precision: toUInt256(1e38) == 99999999999999997752612184630461283328
SELECT multiplyDecimal(toDecimal256(1e38, 0), toDecimal256(1e38, 0));
SELECT divideDecimal(toDecimal256(1e66, 0), toDecimal256(1e-10, 10), 0);

-- fits Decimal256, but scale is too big to fit
SELECT multiplyDecimal(toDecimal256(1e38, 0), toDecimal256(1e38, 0), 2); -- { serverError DECIMAL_OVERFLOW }
SELECT divideDecimal(toDecimal256(1e72, 0), toDecimal256(1e-5, 5), 2); -- { serverError DECIMAL_OVERFLOW }

-- does not fit Decimal256
SELECT multiplyDecimal(toDecimal256('1e38', 0), toDecimal256('1e38', 0)); -- { serverError DECIMAL_OVERFLOW }
SELECT multiplyDecimal(toDecimal256(1e39, 0), toDecimal256(1e39, 0), 0); -- { serverError DECIMAL_OVERFLOW }
SELECT divideDecimal(toDecimal256(1e39, 0), toDecimal256(1e-38, 39)); -- { serverError DECIMAL_OVERFLOW }

-- test different signs
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

-- check against non-const columns
SELECT sum(multiplyDecimal(toDecimal64(number, 1), toDecimal64(number, 5))) FROM numbers(1000);
SELECT sum(divideDecimal(toDecimal64(number, 1), toDecimal64(number, 5))) FROM (select * from numbers(1000) OFFSET 1);

-- check against Nullable type
SELECT multiplyDecimal(toNullable(toDecimal64(10, 1)), toDecimal64(100, 5));
SELECT multiplyDecimal(toDecimal64(10, 1), toNullable(toDecimal64(100, 5)));
SELECT multiplyDecimal(toNullable(toDecimal64(10, 1)), toNullable(toDecimal64(100, 5)));
SELECT divideDecimal(toNullable(toDecimal64(10, 1)), toDecimal64(100, 5));
SELECT divideDecimal(toDecimal64(10, 1), toNullable(toDecimal64(100, 5)));
SELECT divideDecimal(toNullable(toDecimal64(10, 1)), toNullable(toDecimal64(100, 5)));
