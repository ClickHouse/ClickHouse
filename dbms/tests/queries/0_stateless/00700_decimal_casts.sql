SET allow_experimental_decimal_type = 1;
SET send_logs_level = 'none';

SELECT toDecimal32('1.1', 1), toDecimal32('1.1', 2), toDecimal32('1.1', 8);
SELECT toDecimal32('1.1', 0); -- { serverError 69 }
SELECT toDecimal32(1.1, 0), toDecimal32(1.1, 1), toDecimal32(1.1, 2), toDecimal32(1.1, 8);

SELECT toFloat32(9999999)   as x, toDecimal32(x, 0), toDecimal32(-x, 0), toDecimal64(x, 0), toDecimal64(-x, 0);
SELECT toFloat32(999999.9)  as x, toDecimal32(x, 1), toDecimal32(-x, 1), toDecimal64(x, 1), toDecimal64(-x, 1);
SELECT toFloat32(99999.99)  as x, toDecimal32(x, 2), toDecimal32(-x, 2), toDecimal64(x, 2), toDecimal64(-x, 2);
SELECT toFloat32(9999.999)  as x, toDecimal32(x, 3), toDecimal32(-x, 3), toDecimal64(x, 3), toDecimal64(-x, 3);
SELECT toFloat32(999.9999)  as x, toDecimal32(x, 4), toDecimal32(-x, 4), toDecimal64(x, 4), toDecimal64(-x, 4);
SELECT toFloat32(99.99999)  as x, toDecimal32(x, 5), toDecimal32(-x, 5), toDecimal64(x, 5), toDecimal64(-x, 5);
SELECT toFloat32(9.999999)  as x, toDecimal32(x, 6), toDecimal32(-x, 6), toDecimal64(x, 6), toDecimal64(-x, 6);
SELECT toFloat32(0.9999999) as x, toDecimal32(x, 7), toDecimal32(-x, 7), toDecimal64(x, 7), toDecimal64(-x, 7);

SELECT toFloat64(999999999)   as x, toDecimal32(x, 0), toDecimal32(-x, 0), toDecimal64(x, 0), toDecimal64(-x, 0);
SELECT toFloat64(99999999.9)  as x, toDecimal32(x, 1), toDecimal32(-x, 1), toDecimal64(x, 1), toDecimal64(-x, 1);
SELECT toFloat64(9999999.99)  as x, toDecimal32(x, 2), toDecimal32(-x, 2), toDecimal64(x, 2), toDecimal64(-x, 2);
SELECT toFloat64(999999.999)  as x, toDecimal32(x, 3), toDecimal32(-x, 3), toDecimal64(x, 3), toDecimal64(-x, 3);
SELECT toFloat64(99999.9999)  as x, toDecimal32(x, 4), toDecimal32(-x, 4), toDecimal64(x, 4), toDecimal64(-x, 4);
SELECT toFloat64(9999.99999)  as x, toDecimal32(x, 5), toDecimal32(-x, 5), toDecimal64(x, 5), toDecimal64(-x, 5);
SELECT toFloat64(999.999999)  as x, toDecimal32(x, 6), toDecimal32(-x, 6), toDecimal64(x, 6), toDecimal64(-x, 6);
SELECT toFloat64(99.9999999)  as x, toDecimal32(x, 7), toDecimal32(-x, 7), toDecimal64(x, 7), toDecimal64(-x, 7);
SELECT toFloat64(9.99999999)  as x, toDecimal32(x, 8), toDecimal32(-x, 8), toDecimal64(x, 8), toDecimal64(-x, 8);
SELECT toFloat64(0.999999999) as x, toDecimal32(x, 9), toDecimal32(-x, 9), toDecimal64(x, 9), toDecimal64(-x, 9);

SELECT toDecimal32(number, 4) as n1, toDecimal32(n1 / 9, 2) as n2, toDecimal32(n2, 8) FROM system.numbers LIMIT 10;
SELECT toDecimal32(number, 4) as n1, toDecimal32(n1 / 9, 8) as n2, toDecimal32(n2, 2) FROM system.numbers LIMIT 10;
SELECT toDecimal32(number, 8) as n1, toDecimal32(n1 / 9, 4) as n2, toDecimal32(n2, 2) FROM system.numbers LIMIT 10;

SELECT toDecimal64(number, 4) as n1, toDecimal64(n1 / 9, 2) as n2, toDecimal64(n2, 8) FROM system.numbers LIMIT 10;
SELECT toDecimal64(number, 4) as n1, toDecimal64(n1 / 9, 8) as n2, toDecimal64(n2, 2) FROM system.numbers LIMIT 10;
SELECT toDecimal64(number, 8) as n1, toDecimal64(n1 / 9, 4) as n2, toDecimal64(n2, 2) FROM system.numbers LIMIT 10;

SELECT toInt8(99) as x, toDecimal32(x, 0), toDecimal32(-x, 0), toDecimal64(x, 0), toDecimal64(-x, 0);
SELECT toInt16(9999) as x, toDecimal32(x, 0), toDecimal32(-x, 0), toDecimal64(x, 0), toDecimal64(-x, 0);
SELECT toInt32(999999999) as x, toDecimal32(x, 0), toDecimal32(-x, 0), toDecimal64(x, 0), toDecimal64(-x, 0);
SELECT toInt64(999999999) as x, toDecimal32(x, 0), toDecimal32(-x, 0), toDecimal64(x, 0), toDecimal64(-x, 0);
SELECT toInt64(999999999999999999) as x, toDecimal64(x, 0), toDecimal64(-x, 0);

SELECT toUInt8(99) as x, toDecimal32(x, 0), toDecimal64(x, 0);
SELECT toUInt16(9999) as x, toDecimal32(x, 0), toDecimal64(x, 0);
SELECT toUInt32(999999999) as x, toDecimal32(x, 0), toDecimal64(x, 0);
SELECT toUInt64(999999999) as x, toDecimal32(x, 0), toDecimal64(x, 0);

--SELECT CAST('1.1', 'Decimal(9,0)'), CAST('1.1', 'Decimal(9,1)'), CAST('1.1', 'Decimal(9,2)');

--SELECT * FROM test.decimal;
--DROP TABLE IF EXISTS test.decimal;
