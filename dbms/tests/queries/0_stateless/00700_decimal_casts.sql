SET allow_experimental_decimal_type = 1;
SET send_logs_level = 'none';

SELECT toDecimal9('1.1', 1), toDecimal9('1.1', 2), toDecimal9('1.1', 8);
SELECT toDecimal9('1.1', 0); -- { serverError 69 }
SELECT toDecimal9(1.1, 0), toDecimal9(1.1, 1), toDecimal9(1.1, 2), toDecimal9(1.1, 8);

SELECT toFloat32(9999999)   as x, toDecimal9(x, 0), toDecimal9(-x, 0), toDecimal18(x, 0), toDecimal18(-x, 0);
SELECT toFloat32(999999.9)  as x, toDecimal9(x, 1), toDecimal9(-x, 1), toDecimal18(x, 1), toDecimal18(-x, 1);
SELECT toFloat32(99999.99)  as x, toDecimal9(x, 2), toDecimal9(-x, 2), toDecimal18(x, 2), toDecimal18(-x, 2);
SELECT toFloat32(9999.999)  as x, toDecimal9(x, 3), toDecimal9(-x, 3), toDecimal18(x, 3), toDecimal18(-x, 3);
SELECT toFloat32(999.9999)  as x, toDecimal9(x, 4), toDecimal9(-x, 4), toDecimal18(x, 4), toDecimal18(-x, 4);
SELECT toFloat32(99.99999)  as x, toDecimal9(x, 5), toDecimal9(-x, 5), toDecimal18(x, 5), toDecimal18(-x, 5);
SELECT toFloat32(9.999999)  as x, toDecimal9(x, 6), toDecimal9(-x, 6), toDecimal18(x, 6), toDecimal18(-x, 6);
SELECT toFloat32(0.9999999) as x, toDecimal9(x, 7), toDecimal9(-x, 7), toDecimal18(x, 7), toDecimal18(-x, 7);

SELECT toFloat64(999999999)   as x, toDecimal9(x, 0), toDecimal9(-x, 0), toDecimal18(x, 0), toDecimal18(-x, 0);
SELECT toFloat64(99999999.9)  as x, toDecimal9(x, 1), toDecimal9(-x, 1), toDecimal18(x, 1), toDecimal18(-x, 1);
SELECT toFloat64(9999999.99)  as x, toDecimal9(x, 2), toDecimal9(-x, 2), toDecimal18(x, 2), toDecimal18(-x, 2);
SELECT toFloat64(999999.999)  as x, toDecimal9(x, 3), toDecimal9(-x, 3), toDecimal18(x, 3), toDecimal18(-x, 3);
SELECT toFloat64(99999.9999)  as x, toDecimal9(x, 4), toDecimal9(-x, 4), toDecimal18(x, 4), toDecimal18(-x, 4);
SELECT toFloat64(9999.99999)  as x, toDecimal9(x, 5), toDecimal9(-x, 5), toDecimal18(x, 5), toDecimal18(-x, 5);
SELECT toFloat64(999.999999)  as x, toDecimal9(x, 6), toDecimal9(-x, 6), toDecimal18(x, 6), toDecimal18(-x, 6);
SELECT toFloat64(99.9999999)  as x, toDecimal9(x, 7), toDecimal9(-x, 7), toDecimal18(x, 7), toDecimal18(-x, 7);
SELECT toFloat64(9.99999999)  as x, toDecimal9(x, 8), toDecimal9(-x, 8), toDecimal18(x, 8), toDecimal18(-x, 8);
SELECT toFloat64(0.999999999) as x, toDecimal9(x, 9), toDecimal9(-x, 9), toDecimal18(x, 9), toDecimal18(-x, 9);

SELECT toDecimal9(number, 4) as n1, toDecimal9(n1 / 9, 2) as n2, toDecimal9(n2, 8) FROM system.numbers LIMIT 10;
SELECT toDecimal9(number, 4) as n1, toDecimal9(n1 / 9, 8) as n2, toDecimal9(n2, 2) FROM system.numbers LIMIT 10;
SELECT toDecimal9(number, 8) as n1, toDecimal9(n1 / 9, 4) as n2, toDecimal9(n2, 2) FROM system.numbers LIMIT 10;

SELECT toDecimal18(number, 4) as n1, toDecimal18(n1 / 9, 2) as n2, toDecimal18(n2, 8) FROM system.numbers LIMIT 10;
SELECT toDecimal18(number, 4) as n1, toDecimal18(n1 / 9, 8) as n2, toDecimal18(n2, 2) FROM system.numbers LIMIT 10;
SELECT toDecimal18(number, 8) as n1, toDecimal18(n1 / 9, 4) as n2, toDecimal18(n2, 2) FROM system.numbers LIMIT 10;

SELECT toInt8(99) as x, toDecimal9(x, 0), toDecimal9(-x, 0), toDecimal18(x, 0), toDecimal18(-x, 0);
SELECT toInt16(9999) as x, toDecimal9(x, 0), toDecimal9(-x, 0), toDecimal18(x, 0), toDecimal18(-x, 0);
SELECT toInt32(999999999) as x, toDecimal9(x, 0), toDecimal9(-x, 0), toDecimal18(x, 0), toDecimal18(-x, 0);
SELECT toInt64(999999999) as x, toDecimal9(x, 0), toDecimal9(-x, 0), toDecimal18(x, 0), toDecimal18(-x, 0);
SELECT toInt64(999999999999999999) as x, toDecimal18(x, 0), toDecimal18(-x, 0);

SELECT toUInt8(99) as x, toDecimal9(x, 0), toDecimal18(x, 0);
SELECT toUInt16(9999) as x, toDecimal9(x, 0), toDecimal18(x, 0);
SELECT toUInt32(999999999) as x, toDecimal9(x, 0), toDecimal18(x, 0);
SELECT toUInt64(999999999) as x, toDecimal9(x, 0), toDecimal18(x, 0);

--SELECT CAST('1.1', 'Decimal(9,0)'), CAST('1.1', 'Decimal(9,1)'), CAST('1.1', 'Decimal(9,2)');

--SELECT * FROM test.decimal;
--DROP TABLE IF EXISTS test.decimal;
