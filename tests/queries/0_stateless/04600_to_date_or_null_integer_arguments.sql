-- Integer arguments for toDateOrNull, toDateTimeOrNull and toDateTime64OrNull.
-- The value is interpreted the same way as by toDate, toDateTime and toDateTime64,
-- but values out of range of the result type produce NULL.

SELECT toDateTimeOrNull(1583851242, 'Asia/Shanghai');
SELECT toDateTimeOrNull(1583851242, 'UTC');
SELECT toDateTimeOrNull(1583851242) SETTINGS session_timezone = 'UTC';
SELECT toTypeName(toDateTimeOrNull(1583851242, 'UTC'));
SELECT toDateTimeOrNull(materialize(1583851242), 'UTC');

-- All native integer types are supported.
SELECT toDateTimeOrNull(toUInt8(200), 'UTC'), toDateTimeOrNull(toUInt16(60000), 'UTC'), toDateTimeOrNull(toUInt32(1583851242), 'UTC'), toDateTimeOrNull(toUInt64(1583851242), 'UTC');
SELECT toDateTimeOrNull(toInt8(100), 'UTC'), toDateTimeOrNull(toInt16(30000), 'UTC'), toDateTimeOrNull(toInt32(1583851242), 'UTC'), toDateTimeOrNull(toInt64(1583851242), 'UTC');

-- NULL only for values out of range of DateTime.
SELECT toDateTimeOrNull(toInt64(-1), 'UTC'), toDateTimeOrNull(toInt32(-1), 'UTC'), toDateTimeOrNull(toUInt64(4294967296), 'UTC'), toDateTimeOrNull(toUInt64(4294967295), 'UTC');
SELECT toDateTimeOrNull(toUInt64(18446744073709551615), 'UTC'), toDateTimeOrNull(toInt64(-9223372036854775808), 'UTC');

-- Date: values not exceeding 65535 are day numbers, larger values are Unix timestamps.
SELECT toDateOrNull(18000), toDateOrNull(65535);
SELECT toDateOrNull(1583851242) SETTINGS session_timezone = 'UTC';
SELECT toDateOrNull(toInt8(-1)), toDateOrNull(toInt64(-1)), toDateOrNull(toUInt64(4294967296)), toDateOrNull(toUInt64(4294967295)) SETTINGS session_timezone = 'UTC';
SELECT toTypeName(toDateOrNull(18000));

-- DateTime64: the range is wider and includes negative timestamps.
SELECT toDateTime64OrNull(1583851242, 3, 'UTC');
SELECT toDateTime64OrNull(1583851242) SETTINGS session_timezone = 'UTC';
SELECT toTypeName(toDateTime64OrNull(1583851242, 3, 'UTC'));
SELECT toDateTime64OrNull(toInt64(-1), 3, 'UTC'), toDateTime64OrNull(toInt64(-62167219200), 3, 'UTC'), toDateTime64OrNull(toInt64(-62167219201), 3, 'UTC');
SELECT toDateTime64OrNull(toInt64(253402300799), 3, 'UTC'), toDateTime64OrNull(toInt64(253402300800), 3, 'UTC'), toDateTime64OrNull(toInt64(9223372036854775807), 3, 'UTC');
SELECT toDateTime64OrNull(toUInt8(255), 3, 'UTC'), toDateTime64OrNull(toUInt16(65535), 3, 'UTC'), toDateTime64OrNull(toUInt32(4294967295), 3, 'UTC');
-- Overflow of the value scaled to a high precision produces NULL as well.
SELECT toDateTime64OrNull(10413791999, 9, 'UTC');
-- With zero precision the function returns DateTime, like toDateTime64.
SELECT toDateTime64OrNull(1583851242, 0, 'UTC'), toTypeName(toDateTime64OrNull(1583851242, 0, 'UTC'));

-- Non-constant columns and NULL values.
SELECT toDateTimeOrNull(materialize(x), 'UTC') FROM (SELECT arrayJoin([toInt64(0), 1583851242, -1, 4294967295]) AS x);
SELECT toDateTimeOrNull(x, 'UTC') FROM (SELECT arrayJoin([toNullable(toInt64(1583851242)), NULL]) AS x);

-- String arguments continue to work.
SELECT toDateTimeOrNull('2020-03-10 14:40:42', 'UTC'), toDateTimeOrNull('invalid', 'UTC'), toDateOrNull('2020-03-10'), toDateOrNull('invalid'), toDateTime64OrNull('2020-03-10 14:40:42.123', 3, 'UTC');

-- Unsupported argument types still throw.
SELECT toDateTimeOrNull(toInt128(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDateTimeOrNull(toUInt128(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDateTimeOrNull(1.5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDateOrNull(toUInt256(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDateTime64OrNull(toInt128(1), 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Other conversion functions do not accept integer arguments.
SELECT toUInt32OrNull(123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDate32OrNull(123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT parseDateTimeBestEffortOrNull(123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDateTimeOrZero(1583851242); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toDateOrZero(1583851242); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
