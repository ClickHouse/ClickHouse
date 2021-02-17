SELECT 'Into String';
SELECT reinterpret(49, 'String');
SELECT 'Into FixedString';
SELECT reinterpret(49, 'FixedString(1)');
SELECT reinterpret(49, 'FixedString(2)');
SELECT reinterpret(49, 'FixedString(3)');
SELECT reinterpret(49, 'FixedString(4)');
SELECT reinterpretAsFixedString(49);
SELECT 'Into Numeric Representable';
SELECT 'Integer and Integer types';
SELECT reinterpret(257, 'UInt8'), reinterpretAsUInt8(257);
SELECT reinterpret(257, 'Int8'), reinterpretAsInt8(257);
SELECT reinterpret(257, 'UInt16'), reinterpretAsUInt16(257);
SELECT reinterpret(257, 'Int16'), reinterpretAsInt16(257);
SELECT reinterpret(257, 'UInt32'), reinterpretAsUInt32(257);
SELECT reinterpret(257, 'Int32'), reinterpretAsInt32(257);
SELECT reinterpret(257, 'UInt64'), reinterpretAsUInt64(257);
SELECT reinterpret(257, 'Int64'), reinterpretAsInt64(257);
SELECT reinterpret(257, 'Int128'), reinterpretAsInt128(257);
SELECT reinterpret(257, 'UInt256'), reinterpretAsUInt256(257);
SELECT reinterpret(257, 'Int256'), reinterpretAsInt256(257);
SELECT 'Integer and Float types';
SELECT reinterpret(toFloat32(0.2), 'UInt32'), reinterpretAsUInt32(toFloat32(0.2));
SELECT reinterpret(toFloat64(0.2), 'UInt64'), reinterpretAsUInt64(toFloat64(0.2));
SELECT reinterpretAsFloat32(a), reinterpretAsUInt32(toFloat32(0.2)) as a;
SELECT reinterpretAsFloat64(a), reinterpretAsUInt64(toFloat64(0.2)) as a;
SELECT 'Integer and String types';
SELECT reinterpret(a, 'String'), reinterpretAsString(a), reinterpretAsUInt8('1') as a;
SELECT reinterpret(a, 'String'), reinterpretAsString(a), reinterpretAsUInt8('11') as a;
SELECT reinterpret(a, 'String'), reinterpretAsString(a), reinterpretAsUInt16('11') as a;
SELECT 'ReinterpretErrors';
SELECT reinterpret(toDecimal64(1, 2), 'UInt8'); -- {serverError 43}
SELECT reinterpret('123', 'FixedString(1)'); -- {serverError 43}
SELECT reinterpret(toDateTime('9922337203.6854775808', 1), 'Decimal64(1)'); -- {serverError 43}
