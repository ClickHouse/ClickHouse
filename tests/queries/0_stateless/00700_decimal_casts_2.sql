SELECT toDecimal128('1234567890', 28) AS x, toDecimal128(x, 29), toDecimal128(toDecimal128('1234567890', 28), 29);
SELECT toDecimal128(toDecimal128('1234567890', 28), 30);

SELECT toDecimal64('1234567890', 8) AS x, toDecimal64(x, 9), toDecimal64(toDecimal64('1234567890', 8), 9);
SELECT toDecimal64(toDecimal64('1234567890', 8), 10); -- { serverError DECIMAL_OVERFLOW }

SELECT toDecimal32('12345678', 1) AS x, toDecimal32(x, 2), toDecimal32(toDecimal32('12345678', 1), 2);
SELECT toDecimal32(toDecimal32('12345678', 1), 3); -- { serverError DECIMAL_OVERFLOW }

SELECT toDecimal64(toDecimal64('92233720368547758.1', 1), 2); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal64(toDecimal64('-92233720368547758.1', 1), 2); -- { serverError DECIMAL_OVERFLOW }

SELECT toDecimal128('9223372036854775807', 6) AS x, toInt64(x), toInt64(-x);
SELECT toDecimal128('9223372036854775809', 6) AS x, toInt64(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal128('9223372036854775809', 6) AS x, toInt64(-x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal64('922337203685477580', 0) * 10 AS x, toInt64(x), toInt64(-x);
SELECT toDecimal64(toDecimal64('92233720368547758.0', 1), 2) AS x, toInt64(x), toInt64(-x);

SELECT toDecimal128('2147483647', 10) AS x, toInt32(x), toInt32(-x);
SELECT toDecimal128('2147483649', 10) AS x, toInt32(x), toInt32(-x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal64('2147483647', 2) AS x, toInt32(x), toInt32(-x);
SELECT toDecimal64('2147483649', 2) AS x, toInt32(x), toInt32(-x); -- { serverError DECIMAL_OVERFLOW }

SELECT toDecimal128('92233720368547757.99', 2) AS x, toInt64(x), toInt64(-x);
SELECT toDecimal64('2147483640.99', 2) AS x, toInt32(x), toInt32(-x);

SELECT toDecimal128('-0.9', 8) AS x, toUInt64(x);
SELECT toDecimal64('-0.9', 8) AS x, toUInt64(x);
SELECT toDecimal32('-0.9', 8) AS x, toUInt64(x);

SELECT toDecimal128('-0.8', 4) AS x, toUInt32(x);
SELECT toDecimal64('-0.8', 4) AS x, toUInt32(x);
SELECT toDecimal32('-0.8', 4) AS x, toUInt32(x);

SELECT toDecimal128('-0.7', 2) AS x, toUInt16(x);
SELECT toDecimal64('-0.7', 2) AS x, toUInt16(x);
SELECT toDecimal32('-0.7', 2) AS x, toUInt16(x);

SELECT toDecimal128('-0.6', 6) AS x, toUInt8(x);
SELECT toDecimal64('-0.6', 6) AS x, toUInt8(x);
SELECT toDecimal32('-0.6', 6) AS x, toUInt8(x);

SELECT toDecimal128('-1', 7) AS x, toUInt64(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal128('-1', 7) AS x, toUInt32(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal128('-1', 7) AS x, toUInt16(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal128('-1', 7) AS x, toUInt8(x); -- { serverError DECIMAL_OVERFLOW }

SELECT toDecimal64('-1', 5) AS x, toUInt64(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal64('-1', 5) AS x, toUInt32(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal64('-1', 5) AS x, toUInt16(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal64('-1', 5) AS x, toUInt8(x); -- { serverError DECIMAL_OVERFLOW }

SELECT toDecimal32('-1', 3) AS x, toUInt64(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal32('-1', 3) AS x, toUInt32(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal32('-1', 3) AS x, toUInt16(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal32('-1', 3) AS x, toUInt8(x); -- { serverError DECIMAL_OVERFLOW }

SELECT toDecimal128('18446744073709551615', 0) AS x, toUInt64(x);
SELECT toDecimal128('18446744073709551616', 0) AS x, toUInt64(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal128('18446744073709551615', 8) AS x, toUInt64(x);
SELECT toDecimal128('18446744073709551616', 8) AS x, toUInt64(x); -- { serverError DECIMAL_OVERFLOW }

SELECT toDecimal128('4294967295', 0) AS x, toUInt32(x);
SELECT toDecimal128('4294967296', 0) AS x, toUInt32(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal128('4294967295', 10) AS x, toUInt32(x);
SELECT toDecimal128('4294967296', 10) AS x, toUInt32(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal64('4294967295', 0) AS x, toUInt32(x);
SELECT toDecimal64('4294967296', 0) AS x, toUInt32(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal64('4294967295', 4) AS x, toUInt32(x);
SELECT toDecimal64('4294967296', 4) AS x, toUInt32(x); -- { serverError DECIMAL_OVERFLOW }

SELECT toDecimal128('65535', 0) AS x, toUInt16(x);
SELECT toDecimal128('65536', 0) AS x, toUInt16(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal128('65535', 10) AS x, toUInt16(x);
SELECT toDecimal128('65536', 10) AS x, toUInt16(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal64('65535', 0) AS x, toUInt16(x);
SELECT toDecimal64('65536', 0) AS x, toUInt16(x); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal64('65535', 4) AS x, toUInt16(x);
SELECT toDecimal64('65536', 4) AS x, toUInt16(x); -- { serverError DECIMAL_OVERFLOW }

SELECT toInt64('2147483647') AS x, toDecimal32(x, 0);
SELECT toInt64('-2147483647') AS x, toDecimal32(x, 0);
SELECT toUInt64('2147483647') AS x, toDecimal32(x, 0);
SELECT toInt64('2147483649') AS x, toDecimal32(x, 0); -- { serverError DECIMAL_OVERFLOW }
SELECT toInt64('-2147483649') AS x, toDecimal32(x, 0); -- { serverError DECIMAL_OVERFLOW }
SELECT toUInt64('2147483649') AS x, toDecimal32(x, 0); -- { serverError DECIMAL_OVERFLOW }

SELECT toUInt64('9223372036854775807') AS x, toDecimal64(x, 0);
SELECT toUInt64('9223372036854775809') AS x, toDecimal64(x, 0); -- { serverError DECIMAL_OVERFLOW }

SELECT toDecimal32(0, rowNumberInBlock()); -- { serverError ILLEGAL_COLUMN }
SELECT toDecimal64(0, rowNumberInBlock()); -- { serverError ILLEGAL_COLUMN }
SELECT toDecimal128(0, rowNumberInBlock()); -- { serverError ILLEGAL_COLUMN }

SELECT toDecimal32(1/0, 0); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal64(1/0, 1); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal128(0/0, 2); -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(1/0, 'Decimal(9, 0)'); -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(1/0, 'Decimal(18, 1)'); -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(1/0, 'Decimal(38, 2)'); -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(0/0, 'Decimal(9, 3)'); -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(0/0, 'Decimal(18, 4)'); -- { serverError DECIMAL_OVERFLOW }
SELECT CAST(0/0, 'Decimal(38, 5)'); -- { serverError DECIMAL_OVERFLOW }

select toDecimal32(10000.1, 6); -- { serverError DECIMAL_OVERFLOW }
select toDecimal64(10000.1, 18); -- { serverError DECIMAL_OVERFLOW }
select toDecimal128(1000000000000000000000.1, 18); -- { serverError DECIMAL_OVERFLOW }

select toDecimal32(-10000.1, 6); -- { serverError DECIMAL_OVERFLOW }
select toDecimal64(-10000.1, 18); -- { serverError DECIMAL_OVERFLOW }
select toDecimal128(-1000000000000000000000.1, 18); -- { serverError DECIMAL_OVERFLOW }

select toDecimal32(2147483647.0 + 1.0, 0); -- { serverError DECIMAL_OVERFLOW }
select toDecimal64(9223372036854775807.0, 0); -- { serverError DECIMAL_OVERFLOW }
select toDecimal128(170141183460469231731687303715884105729.0, 0); -- { serverError DECIMAL_OVERFLOW }

select toDecimal32(-2147483647.0 - 1.0, 0); -- { serverError DECIMAL_OVERFLOW }
select toDecimal64(-9223372036854775807.0, 0); -- { serverError DECIMAL_OVERFLOW }
select toDecimal128(-170141183460469231731687303715884105729.0, 0); -- { serverError DECIMAL_OVERFLOW }
