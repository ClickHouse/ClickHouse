SELECT toDecimal32('1.1', 1), toDecimal32('1.1', 2), toDecimal32('1.1', 8);
SELECT toDecimal32('1.1', 0);
SELECT toDecimal32(1.1, 0), toDecimal32(1.1, 1), toDecimal32(1.1, 2), toDecimal32(1.1, 8);

SELECT '1000000000' AS x, toDecimal32(x, 0); -- { serverError 69 }
SELECT '-1000000000' AS x, toDecimal32(x, 0); -- { serverError 69 }
SELECT '1000000000000000000' AS x, toDecimal64(x, 0); -- { serverError 69 }
SELECT '-1000000000000000000' AS x, toDecimal64(x, 0); -- { serverError 69 }
SELECT '100000000000000000000000000000000000000' AS x, toDecimal128(x, 0); -- { serverError 69 }
SELECT '-100000000000000000000000000000000000000' AS x, toDecimal128(x, 0); -- { serverError 69 }
SELECT '1' AS x, toDecimal32(x, 9); -- { serverError 69 }
SELECT '-1' AS x, toDecimal32(x, 9); -- { serverError 69 }
SELECT '1' AS x, toDecimal64(x, 18); -- { serverError 69 }
SELECT '-1' AS x, toDecimal64(x, 18); -- { serverError 69 }
SELECT '1' AS x, toDecimal128(x, 38); -- { serverError 69 }
SELECT '-1' AS x, toDecimal128(x, 38); -- { serverError 69 }

SELECT '0.1' AS x, toDecimal32(x, 0);
SELECT '-0.1' AS x, toDecimal32(x, 0);
SELECT '0.1' AS x, toDecimal64(x, 0);
SELECT '-0.1' AS x, toDecimal64(x, 0);
SELECT '0.1' AS x, toDecimal128(x, 0);
SELECT '-0.1' AS x, toDecimal128(x, 0);
SELECT '0.0000000001' AS x, toDecimal32(x, 9);
SELECT '-0.0000000001' AS x, toDecimal32(x, 9);
SELECT '0.0000000000000000001' AS x, toDecimal64(x, 18);
SELECT '-0.0000000000000000001' AS x, toDecimal64(x, 18);
SELECT '0.000000000000000000000000000000000000001' AS x, toDecimal128(x, 38);
SELECT '-0.000000000000000000000000000000000000001' AS x, toDecimal128(x, 38);

SELECT '1e9' AS x, toDecimal32(x, 0); -- { serverError 69 }
SELECT '-1E9' AS x, toDecimal32(x, 0); -- { serverError 69 }
SELECT '1E18' AS x, toDecimal64(x, 0); -- { serverError 69 }
SELECT '-1e18' AS x, toDecimal64(x, 0); -- { serverError 69 }
SELECT '1e38' AS x, toDecimal128(x, 0); -- { serverError 69 }
SELECT '-1E38' AS x, toDecimal128(x, 0); -- { serverError 69 }
SELECT '1e0' AS x, toDecimal32(x, 9); -- { serverError 69 }
SELECT '-1e-0' AS x, toDecimal32(x, 9); -- { serverError 69 }
SELECT '1e0' AS x, toDecimal64(x, 18); -- { serverError 69 }
SELECT '-1e-0' AS x, toDecimal64(x, 18); -- { serverError 69 }
SELECT '1e-0' AS x, toDecimal128(x, 38); -- { serverError 69 }
SELECT '-1e0' AS x, toDecimal128(x, 38); -- { serverError 69 }

SELECT '1e-1' AS x, toDecimal32(x, 0);
SELECT '-1e-1' AS x, toDecimal32(x, 0);
SELECT '1e-1' AS x, toDecimal64(x, 0);
SELECT '-1e-1' AS x, toDecimal64(x, 0);
SELECT '1e-1' AS x, toDecimal128(x, 0);
SELECT '-1e-1' AS x, toDecimal128(x, 0);
SELECT '1e-10' AS x, toDecimal32(x, 9);
SELECT '-1e-10' AS x, toDecimal32(x, 9);
SELECT '1e-19' AS x, toDecimal64(x, 18);
SELECT '-1e-19' AS x, toDecimal64(x, 18);
SELECT '1e-39' AS x, toDecimal128(x, 38);
SELECT '-1e-39' AS x, toDecimal128(x, 38);

SELECT toFloat32(9999999)   as x, toDecimal32(x, 0), toDecimal32(-x, 0), toDecimal64(x, 0), toDecimal64(-x, 0);
SELECT toFloat32(999999.9)  as x, toDecimal32(x, 1), toDecimal32(-x, 1), toDecimal64(x, 1), toDecimal64(-x, 1);
SELECT toFloat32(99999.99)  as x, toDecimal32(x, 2), toDecimal32(-x, 2), toDecimal64(x, 2), toDecimal64(-x, 2);
SELECT toFloat32(9999.999)  as x, toDecimal32(x, 3), toDecimal32(-x, 3), toDecimal64(x, 3), toDecimal64(-x, 3);
SELECT toFloat32(999.9999)  as x, toDecimal32(x, 4), toDecimal32(-x, 4), toDecimal64(x, 4), toDecimal64(-x, 4);
SELECT toFloat32(99.99999)  as x, toDecimal32(x, 5), toDecimal32(-x, 5), toDecimal64(x, 5), toDecimal64(-x, 5);
SELECT toFloat32(9.999999)  as x, toDecimal32(x, 6), toDecimal32(-x, 6), toDecimal64(x, 6), toDecimal64(-x, 6);
SELECT toFloat32(0.9999999) as x, toDecimal32(x, 7), toDecimal32(-x, 7), toDecimal64(x, 7), toDecimal64(-x, 7);

SELECT toFloat32(9.99999999)  as x, toDecimal32(x, 8), toDecimal32(-x, 8), toDecimal64(x, 8), toDecimal64(-x, 8);
SELECT toFloat32(0.999999999) as x, toDecimal32(x, 9), toDecimal32(-x, 9), toDecimal64(x, 9), toDecimal64(-x, 9);

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

SELECT toFloat64(999999999.999999999)  as x, toDecimal64(x, 9), toDecimal64(-x, 9);
SELECT toFloat64(99999999.9999999999)  as x, toDecimal64(x, 10), toDecimal64(-x, 10);
SELECT toFloat64(9999999.99999999999)  as x, toDecimal64(x, 11), toDecimal64(-x, 11);
SELECT toFloat64(999999.999999999999)  as x, toDecimal64(x, 12), toDecimal64(-x, 12);
SELECT toFloat64(99999.9999999999999)  as x, toDecimal64(x, 13), toDecimal64(-x, 13);
SELECT toFloat64(9999.99999999999999)  as x, toDecimal64(x, 14), toDecimal64(-x, 14);
SELECT toFloat64(999.999999999999999)  as x, toDecimal64(x, 15), toDecimal64(-x, 15);
SELECT toFloat64(99.9999999999999999)  as x, toDecimal64(x, 16), toDecimal64(-x, 16);
SELECT toFloat64(9.99999999999999999)  as x, toDecimal64(x, 17), toDecimal64(-x, 17);
SELECT toFloat64(0.999999999999999999) as x, toDecimal64(x, 18), toDecimal64(-x, 18);

SELECT toFloat64(999999999999999999)   as x, toDecimal128(x, 0), toDecimal128(-x, 0);
SELECT toFloat64(99999999999999999.9)  as x, toDecimal128(x, 1), toDecimal128(-x, 1);
SELECT toFloat64(9999999999999999.99)  as x, toDecimal128(x, 2), toDecimal128(-x, 2);
SELECT toFloat64(999999999999999.999)  as x, toDecimal128(x, 3), toDecimal128(-x, 3);
SELECT toFloat64(99999999999999.9999)  as x, toDecimal128(x, 4), toDecimal128(-x, 4);
SELECT toFloat64(9999999999999.99999)  as x, toDecimal128(x, 5), toDecimal128(-x, 5);
SELECT toFloat64(999999999999.999999)  as x, toDecimal128(x, 6), toDecimal128(-x, 6);
SELECT toFloat64(99999999999.9999999)  as x, toDecimal128(x, 7), toDecimal128(-x, 7);
SELECT toFloat64(9999999999.99999999)  as x, toDecimal128(x, 8), toDecimal128(-x, 8);
SELECT toFloat64(999999999.999999999)  as x, toDecimal128(x, 9), toDecimal128(-x, 9);
SELECT toFloat64(999999999.999999999)  as x, toDecimal128(x, 9), toDecimal128(-x, 9);
SELECT toFloat64(99999999.9999999999)  as x, toDecimal128(x, 10), toDecimal128(-x, 10);
SELECT toFloat64(9999999.99999999999)  as x, toDecimal128(x, 11), toDecimal128(-x, 11);
SELECT toFloat64(999999.999999999999)  as x, toDecimal128(x, 12), toDecimal128(-x, 12);
SELECT toFloat64(99999.9999999999999)  as x, toDecimal128(x, 13), toDecimal128(-x, 13);
SELECT toFloat64(9999.99999999999999)  as x, toDecimal128(x, 14), toDecimal128(-x, 14);
SELECT toFloat64(999.999999999999999)  as x, toDecimal128(x, 15), toDecimal128(-x, 15);
SELECT toFloat64(99.9999999999999999)  as x, toDecimal128(x, 16), toDecimal128(-x, 16);
SELECT toFloat64(9.99999999999999999)  as x, toDecimal128(x, 17), toDecimal128(-x, 17);
SELECT toFloat64(0.999999999999999999) as x, toDecimal128(x, 18), toDecimal128(-x, 18);

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
SELECT toInt32(999999999) as x, toDecimal64(x, 9), toDecimal64(-x, 9), toDecimal128(x, 29), toDecimal128(-x, 29);
SELECT toInt64(999999999) as x, toDecimal64(x, 9), toDecimal64(-x, 9), toDecimal128(x, 29), toDecimal128(-x, 29);
SELECT toInt64(999999999999999999) as x, toDecimal64(x, 0), toDecimal64(-x, 0);
SELECT toInt64(999999999999999999) as x, toDecimal128(x, 0), toDecimal128(-x, 0);
SELECT toInt64(999999999999999999) as x, toDecimal128(x, 20), toDecimal128(-x, 20);

SELECT toUInt8(99) as x, toDecimal32(x, 0), toDecimal64(x, 0);
SELECT toUInt16(9999) as x, toDecimal32(x, 0), toDecimal64(x, 0);
SELECT toUInt32(999999999) as x, toDecimal32(x, 0), toDecimal64(x, 0);
SELECT toUInt64(999999999) as x, toDecimal32(x, 0), toDecimal64(x, 0);

SELECT CAST('42.4200', 'Decimal(9,2)') AS a, CAST(a, 'Decimal(9,2)'), CAST(a, 'Decimal(18, 2)'), CAST(a, 'Decimal(38, 2)');
SELECT CAST('42.42', 'Decimal(9,2)') AS a, CAST(a, 'Decimal(9,7)'), CAST(a, 'Decimal(18, 16)'), CAST(a, 'Decimal(38, 36)');

SELECT CAST('123456789', 'Decimal(9,0)'), CAST('123456789123456789', 'Decimal(18,0)');
SELECT CAST('12345678901234567890123456789012345678', 'Decimal(38,0)');
SELECT CAST('123456789', 'Decimal(9,1)'); -- { serverError 69 }
SELECT CAST('123456789123456789', 'Decimal(18,1)'); -- { serverError 69 }
SELECT CAST('12345678901234567890123456789012345678', 'Decimal(38,1)'); -- { serverError 69 }

SELECT CAST('0.123456789', 'Decimal(9,9)'), CAST('0.123456789123456789', 'Decimal(18,18)');
SELECT CAST('0.12345678901234567890123456789012345678', 'Decimal(38,38)');
SELECT CAST('0.123456789', 'Decimal(9,8)');
SELECT CAST('0.123456789123456789', 'Decimal(18,17)');
SELECT CAST('0.12345678901234567890123456789012345678', 'Decimal(38,37)');

SELECT toDecimal128('1234567890', 28) AS x, toDecimal128(x, 29), toDecimal128(toDecimal128('1234567890', 28), 29);
SELECT toDecimal128(toDecimal128('1234567890', 28), 30); -- { serverError 407 }

SELECT toDecimal64('1234567890', 8) AS x, toDecimal64(x, 9), toDecimal64(toDecimal64('1234567890', 8), 9);
SELECT toDecimal64(toDecimal64('1234567890', 8), 10); -- { serverError 407 }

SELECT toDecimal32('12345678', 1) AS x, toDecimal32(x, 2), toDecimal32(toDecimal32('12345678', 1), 2);
SELECT toDecimal32(toDecimal32('12345678', 1), 3); -- { serverError 407 }

SELECT toDecimal64(toDecimal64('92233720368547758.1', 1), 2); -- { serverError 407 }
SELECT toDecimal64(toDecimal64('-92233720368547758.1', 1), 2); -- { serverError 407 }

SELECT toDecimal128('9223372036854775807', 6) AS x, toInt64(x), toInt64(-x);
SELECT toDecimal128('9223372036854775809', 6) AS x, toInt64(x); -- { serverError 407 }
SELECT toDecimal128('9223372036854775809', 6) AS x, toInt64(-x); -- { serverError 407 }
SELECT toDecimal64('922337203685477580', 0) * 10 AS x, toInt64(x), toInt64(-x);
SELECT toDecimal64(toDecimal64('92233720368547758.0', 1), 2) AS x, toInt64(x), toInt64(-x);

SELECT toDecimal128('2147483647', 10) AS x, toInt32(x), toInt32(-x);
SELECT toDecimal128('2147483649', 10) AS x, toInt32(x), toInt32(-x); -- { serverError 407 }
SELECT toDecimal64('2147483647', 2) AS x, toInt32(x), toInt32(-x);
SELECT toDecimal64('2147483649', 2) AS x, toInt32(x), toInt32(-x); -- { serverError 407 }

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

SELECT toDecimal128('-1', 7) AS x, toUInt64(x); -- { serverError 407 }
SELECT toDecimal128('-1', 7) AS x, toUInt32(x); -- { serverError 407 }
SELECT toDecimal128('-1', 7) AS x, toUInt16(x); -- { serverError 407 }
SELECT toDecimal128('-1', 7) AS x, toUInt8(x); -- { serverError 407 }

SELECT toDecimal64('-1', 5) AS x, toUInt64(x); -- { serverError 407 }
SELECT toDecimal64('-1', 5) AS x, toUInt32(x); -- { serverError 407 }
SELECT toDecimal64('-1', 5) AS x, toUInt16(x); -- { serverError 407 }
SELECT toDecimal64('-1', 5) AS x, toUInt8(x); -- { serverError 407 }

SELECT toDecimal32('-1', 3) AS x, toUInt64(x); -- { serverError 407 }
SELECT toDecimal32('-1', 3) AS x, toUInt32(x); -- { serverError 407 }
SELECT toDecimal32('-1', 3) AS x, toUInt16(x); -- { serverError 407 }
SELECT toDecimal32('-1', 3) AS x, toUInt8(x); -- { serverError 407 }

SELECT toDecimal128('18446744073709551615', 0) AS x, toUInt64(x);
SELECT toDecimal128('18446744073709551616', 0) AS x, toUInt64(x); -- { serverError 407 }
SELECT toDecimal128('18446744073709551615', 8) AS x, toUInt64(x);
SELECT toDecimal128('18446744073709551616', 8) AS x, toUInt64(x); -- { serverError 407 }

SELECT toDecimal128('4294967295', 0) AS x, toUInt32(x);
SELECT toDecimal128('4294967296', 0) AS x, toUInt32(x); -- { serverError 407 }
SELECT toDecimal128('4294967295', 10) AS x, toUInt32(x);
SELECT toDecimal128('4294967296', 10) AS x, toUInt32(x); -- { serverError 407 }
SELECT toDecimal64('4294967295', 0) AS x, toUInt32(x);
SELECT toDecimal64('4294967296', 0) AS x, toUInt32(x); -- { serverError 407 }
SELECT toDecimal64('4294967295', 4) AS x, toUInt32(x);
SELECT toDecimal64('4294967296', 4) AS x, toUInt32(x); -- { serverError 407 }

SELECT toDecimal128('65535', 0) AS x, toUInt16(x);
SELECT toDecimal128('65536', 0) AS x, toUInt16(x); -- { serverError 407 }
SELECT toDecimal128('65535', 10) AS x, toUInt16(x);
SELECT toDecimal128('65536', 10) AS x, toUInt16(x); -- { serverError 407 }
SELECT toDecimal64('65535', 0) AS x, toUInt16(x);
SELECT toDecimal64('65536', 0) AS x, toUInt16(x); -- { serverError 407 }
SELECT toDecimal64('65535', 4) AS x, toUInt16(x);
SELECT toDecimal64('65536', 4) AS x, toUInt16(x); -- { serverError 407 }

SELECT toInt64('2147483647') AS x, toDecimal32(x, 0);
SELECT toInt64('-2147483647') AS x, toDecimal32(x, 0);
SELECT toUInt64('2147483647') AS x, toDecimal32(x, 0);
SELECT toInt64('2147483649') AS x, toDecimal32(x, 0); -- { serverError 407 }
SELECT toInt64('-2147483649') AS x, toDecimal32(x, 0); -- { serverError 407 }
SELECT toUInt64('2147483649') AS x, toDecimal32(x, 0); -- { serverError 407 }

SELECT toUInt64('9223372036854775807') AS x, toDecimal64(x, 0);
SELECT toUInt64('9223372036854775809') AS x, toDecimal64(x, 0); -- { serverError 407 }

SELECT toDecimal32(0, rowNumberInBlock()); -- { serverError 44 }
SELECT toDecimal64(0, rowNumberInBlock()); -- { serverError 44 }
SELECT toDecimal128(0, rowNumberInBlock()); -- { serverError 44 }

SELECT toDecimal32(1/0, 0); -- { serverError 407 }
SELECT toDecimal64(1/0, 1); -- { serverError 407 }
SELECT toDecimal128(0/0, 2); -- { serverError 407 }
SELECT CAST(1/0, 'Decimal(9, 0)'); -- { serverError 407 }
SELECT CAST(1/0, 'Decimal(18, 1)'); -- { serverError 407 }
SELECT CAST(1/0, 'Decimal(38, 2)'); -- { serverError 407 }
SELECT CAST(0/0, 'Decimal(9, 3)'); -- { serverError 407 }
SELECT CAST(0/0, 'Decimal(18, 4)'); -- { serverError 407 }
SELECT CAST(0/0, 'Decimal(38, 5)'); -- { serverError 407 }

select toDecimal32(10000.1, 6); -- { serverError 407 }
select toDecimal64(10000.1, 18); -- { serverError 407 }
select toDecimal128(1000000000000000000000.1, 18); -- { serverError 407 }

select toDecimal32(-10000.1, 6); -- { serverError 407 }
select toDecimal64(-10000.1, 18); -- { serverError 407 }
select toDecimal128(-1000000000000000000000.1, 18); -- { serverError 407 }

select toDecimal32(2147483647.0 + 1.0, 0); -- { serverError 407 }
select toDecimal64(9223372036854775807.0, 0); -- { serverError 407 }
select toDecimal128(170141183460469231731687303715884105729.0, 0); -- { serverError 407 }

select toDecimal32(-2147483647.0 - 1.0, 0); -- { serverError 407 }
select toDecimal64(-9223372036854775807.0, 0); -- { serverError 407 }
select toDecimal128(-170141183460469231731687303715884105729.0, 0); -- { serverError 407 }
