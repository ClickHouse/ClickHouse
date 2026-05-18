-- Exercise the OrZero / OrNull / OrDefault and accurate{Cast,CastOrNull,CastOrDefault}
-- branches of Functions/FunctionsConversion.h, across numeric / Decimal / Date /
-- DateTime / Date32 / UUID / IPv4 / IPv6 / FixedString / big-integer types.

-- CI randomizes the session timezone; pin it so zero/default DateTime values
-- render consistently.
SET session_timezone = 'UTC';

SELECT '--- numeric OrZero / OrNull / OrDefault on valid + invalid input ---';
SELECT toInt32OrZero('42'), toInt32OrZero('bad');
SELECT toInt32OrNull('42'), toInt32OrNull('bad');
SELECT toInt32OrDefault('42', -1::Int32), toInt32OrDefault('bad', -1::Int32);

SELECT '--- accurateCast family ---';
SELECT accurateCastOrNull(200, 'Int8');
SELECT accurateCastOrDefault(200, 'Int8');
SELECT accurateCastOrNull(100, 'Int8');

SELECT '--- toDecimal32 / OrZero / OrNull ---';
SELECT toDecimal32('1.5', 2);
SELECT toDecimal32OrZero('bad', 2);
SELECT toDecimal32OrNull('bad', 2);

SELECT '--- toDecimal64 / OrZero / OrNull ---';
SELECT toDecimal64('3.14', 3);
SELECT toDecimal64OrZero('bad', 3);
SELECT toDecimal64OrNull('bad', 3);

SELECT '--- toDecimal128 / OrZero / OrNull ---';
SELECT toDecimal128('2.718', 3);
SELECT toDecimal128OrZero('bad', 3);
SELECT toDecimal128OrNull('bad', 3);

SELECT '--- toDecimal256 ---';
SELECT toDecimal256('1.2345', 4);
SELECT toDecimal256OrZero('bad', 4);
SELECT toDecimal256OrNull('bad', 4);

SELECT '--- toDate32 / OrZero / OrNull ---';
SELECT toDate32('2023-01-02');
SELECT toDate32OrZero('bad');
SELECT toDate32OrNull('bad');

SELECT '--- toDateTime64 / OrZero / OrNull ---';
SELECT toDateTime64('2023-01-02 03:04:05.123', 3, 'UTC');
SELECT toDateTime64OrZero('bad', 3);
SELECT toDateTime64OrNull('bad', 3);

SELECT '--- UUID: OrZero / OrNull / OrDefault ---';
SELECT toUUIDOrZero('bad');
SELECT toUUIDOrNull('bad');
SELECT toUUIDOrDefault('bad', toUUID('11111111-1111-1111-1111-111111111111'));

SELECT '--- IPv4: OrZero / OrNull / OrDefault ---';
SELECT toIPv4OrZero('bad');
SELECT toIPv4OrNull('bad');
SELECT toIPv4OrDefault('bad', toIPv4('1.2.3.4'));

SELECT '--- IPv6: OrZero / OrNull ---';
SELECT toIPv6OrZero('bad');
SELECT toIPv6OrNull('bad');

SELECT '--- IPv4 <-> IPv6 conversions ---';
SELECT toIPv6(toIPv4('1.2.3.4'));
SELECT toIPv4(toIPv6('::ffff:0102:0304'));

SELECT '--- FixedString: conversions and length errors ---';
SELECT toFixedString('abc', 5);
SELECT toFixedString('exact', 5);
SELECT toFixedString('toolong', 3); -- { serverError TOO_LARGE_STRING_SIZE }

SELECT '--- big-integer conversions ---';
SELECT toInt128('123456789012345678901234567890');
SELECT toInt256('12345678901234567890123456789012345');
SELECT toUInt128('340282366920938463463374607431768211455');
SELECT toUInt256('1234567890123456789012345678901234567890');

SELECT '--- Date / DateTime numeric constructors ---';
SELECT toDate(19723);
SELECT toDateTime(1672531200, 'UTC');
SELECT toDateTime64(1672531200::Float64, 3, 'UTC');

SELECT '--- Conversions that yield NULL with tryCast ---';
SELECT toInt32OrNull('x'), toFloat64OrNull('x'), toDecimal32OrNull('x', 3);
SELECT toDate32OrNull('x'), toDateTimeOrNull('x'), toDateTime64OrNull('x', 3);

SELECT '--- explicit casts using CAST(value, type) ---';
SELECT CAST('1.5', 'Decimal(10, 2)');
SELECT CAST('1970-01-01', 'Date');
SELECT CAST('127.0.0.1', 'IPv4');
SELECT CAST('::1', 'IPv6');

SELECT '--- String to array / tuple via CAST ---';
SELECT CAST('[1,2,3]', 'Array(Int32)');
SELECT CAST('(1, ''a'')', 'Tuple(Int32, String)');

SELECT '--- Number to String ---';
SELECT toString(1::Int8), toString(1::Int64), toString(1.5::Float32);
SELECT toString(toDate('2023-01-02')), toString(toDateTime('2023-01-02 03:04:05', 'UTC'));
SELECT toString(toIPv4('1.2.3.4')), toString(toIPv6('::1'));
SELECT toString(toUUID('00000000-0000-0000-0000-000000000001'));

SELECT '--- tryCast that fails returns NULL; CAST throws ---';
SELECT accurateCastOrNull('bad', 'Int32');
SELECT CAST('bad', 'Int32'); -- { serverError CANNOT_PARSE_TEXT }
-- CAST(-1, 'UInt32') is actually accepted (wraps); use accurateCast to force rejection.
SELECT accurateCast(-1, 'UInt32'); -- { serverError CANNOT_CONVERT_TYPE }
