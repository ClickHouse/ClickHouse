SELECT accurateCastOrDefault(-1, 'UInt8'), accurateCastOrDefault(5, 'UInt8');
SELECT accurateCastOrDefault(5, 'UInt8');
SELECT accurateCastOrDefault(257, 'UInt8'),  accurateCastOrDefault(257, 'UInt8', 5);
SELECT accurateCastOrDefault(-1, 'UInt16'),  accurateCastOrDefault(-1, 'UInt16', toUInt16(5));
SELECT accurateCastOrDefault(5, 'UInt16');
SELECT accurateCastOrDefault(65536, 'UInt16'),  accurateCastOrDefault(65536, 'UInt16', toUInt16(5));
SELECT accurateCastOrDefault(-1, 'UInt32'),  accurateCastOrDefault(-1, 'UInt32', toUInt32(5));
SELECT accurateCastOrDefault(5, 'UInt32');
SELECT accurateCastOrDefault(4294967296, 'UInt32'),  accurateCastOrDefault(4294967296, 'UInt32', toUInt32(5));
SELECT accurateCastOrDefault(-1, 'UInt64'), accurateCastOrDefault(-1, 'UInt64', toUInt64(5));
SELECT accurateCastOrDefault(5, 'UInt64');
SELECT accurateCastOrDefault(-1, 'UInt256'), accurateCastOrDefault(-1, 'UInt256', toUInt256(5));
SELECT accurateCastOrDefault(5, 'UInt256');
SELECT accurateCastOrDefault(-129, 'Int8'), accurateCastOrDefault(-129, 'Int8', toInt8(5));
SELECT accurateCastOrDefault(5, 'Int8');
SELECT accurateCastOrDefault(128, 'Int8'),  accurateCastOrDefault(128, 'Int8', toInt8(5));

SELECT accurateCastOrDefault(10, 'Decimal32(9)'), accurateCastOrDefault(10, 'Decimal32(9)', toDecimal32(2, 9));
SELECT accurateCastOrDefault(1, 'Decimal32(9)');
SELECT accurateCastOrDefault(-10, 'Decimal32(9)'), accurateCastOrDefault(-10, 'Decimal32(9)', toDecimal32(2, 9));

SELECT accurateCastOrDefault('123', 'FixedString(2)'), accurateCastOrDefault('123', 'FixedString(2)', cast('12', 'FixedString(2)'));

SELECT accurateCastOrDefault(inf, 'Int64'), accurateCastOrDefault(inf, 'Int64', toInt64(5));
SELECT accurateCastOrDefault(inf, 'Int128'), accurateCastOrDefault(inf, 'Int128', toInt128(5));
SELECT accurateCastOrDefault(inf, 'Int256'), accurateCastOrDefault(inf, 'Int256', toInt256(5));
SELECT accurateCastOrDefault(nan, 'Int64'), accurateCastOrDefault(nan, 'Int64', toInt64(5));
SELECT accurateCastOrDefault(nan, 'Int128'), accurateCastOrDefault(nan, 'Int128', toInt128(5));
SELECT accurateCastOrDefault(nan, 'Int256'), accurateCastOrDefault(nan, 'Int256', toInt256(5));

SELECT accurateCastOrDefault(inf, 'UInt64'), accurateCastOrDefault(inf, 'UInt64', toUInt64(5));
SELECT accurateCastOrDefault(inf, 'UInt256'), accurateCastOrDefault(inf, 'UInt256', toUInt256(5));
SELECT accurateCastOrDefault(nan, 'UInt64'), accurateCastOrDefault(nan, 'UInt64', toUInt64(5));
SELECT accurateCastOrDefault(nan, 'UInt256'), accurateCastOrDefault(nan, 'UInt256', toUInt256(5));

SELECT accurateCastOrDefault(number + 127, 'Int8') AS x, accurateCastOrDefault(number + 127, 'Int8', toInt8(5)) AS x_with_default FROM numbers (2) ORDER BY number;

select accurateCastOrDefault('test', 'Nullable(Bool)');
select accurateCastOrDefault('test', 'Bool');
select accurateCastOrDefault('truex', 'Bool');
select accurateCastOrDefault('xfalse', 'Bool');
select accurateCastOrDefault('true', 'Bool');
select accurateCastOrDefault('false', 'Bool');
select accurateCastOrDefault('1', 'Bool');
select accurateCastOrDefault('0', 'Bool');
select accurateCastOrDefault(1, 'Bool');
select accurateCastOrDefault(0, 'Bool');

select accurateCastOrDefault('test', 'Nullable(IPv4)');
select accurateCastOrDefault('test', 'IPv4');
select accurateCastOrDefault('2001:db8::1', 'IPv4');
select accurateCastOrDefault('::ffff:192.0.2.1', 'IPv4');
select accurateCastOrDefault('192.0.2.1', 'IPv4');
select accurateCastOrDefault('192.0.2.1x', 'IPv4');

select accurateCastOrDefault('test', 'Nullable(IPv6)');
select accurateCastOrDefault('test', 'IPv6');
select accurateCastOrDefault('192.0.2.1', 'IPv6');
select accurateCastOrDefault('2001:db8::1', 'IPv6');
select accurateCastOrDefault('2001:db8::1x', 'IPv6');

-- A failed accurate cast to Enum must report NULL so OrDefault can use its fallback,
-- regardless of the legacy numeric-to-Enum conversion setting.
SET check_conversion_from_numbers_to_enum = 0;
SELECT toInt8(accurateCastOrDefault(-1, $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$)),
       isNull(accurateCastOrNull(-1, $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$));
SELECT accurateCastOrDefault(-1, $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$);
SELECT toInt8(accurateCastOrDefault(-1, $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$, CAST('Hello', $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$)));
SELECT toInt8(CAST(-1, $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$));

SET check_conversion_from_numbers_to_enum = 1;
SELECT toInt8(accurateCastOrDefault(-1, $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$)),
       isNull(accurateCastOrNull(-1, $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$));
SELECT toInt8(accurateCastOrDefault(-1, $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$, CAST('Hello', $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$)));

SELECT toInt8(accurateCastOrDefault('unknown', $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$)),
       isNull(accurateCastOrNull('unknown', $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$)),
       toInt8(accurateCastOrDefault('Hello', $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$));
SELECT toInt8(accurateCastOrDefault('unknown', $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$, CAST('Hello', $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$)));
SELECT toInt16(accurateCastOrDefault(32768, $$Enum16('None' = 1, 'Hello' = 2)$$)),
       isNull(accurateCastOrNull(32768, $$Enum16('None' = 1, 'Hello' = 2)$$));

SELECT number,
       toInt8(accurateCastOrDefault(number + 1, $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$)),
       isNull(accurateCastOrNull(number + 1, $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$))
FROM numbers(4) ORDER BY number;

-- Values outside the target Enum, including in-range integers and fractional numbers.
SELECT toInt8(accurateCastOrDefault(toInt8(0), $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$)),
       isNull(accurateCastOrNull(toInt8(0), $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$));
SELECT toInt8(accurateCastOrDefault(toFloat64(1.5), $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$)),
       isNull(accurateCastOrNull(toFloat64(1.5), $$Enum8('None' = 1, 'Hello' = 2, 'World' = 3)$$));

-- Accurate Enum casts must reject missing enum members and narrowing that wraps a value.
SET check_conversion_from_numbers_to_enum = 0;
WITH
    CAST('large', $$Enum16('large' = 257)$$) AS narrowing,
    CAST('other', $$Enum8('other' = 7)$$) AS same_width,
    CAST('good', $$Enum16('good' = 1)$$) AS valid_source,
    CAST('fallback', $$Enum8('good' = 1, 'fallback' = 2)$$) AS fallback
SELECT
    toInt8(accurateCastOrDefault(narrowing, $$Enum8('good' = 1, 'fallback' = 2)$$, fallback)),
    isNull(accurateCastOrNull(narrowing, $$Enum8('good' = 1, 'fallback' = 2)$$)),
    toInt8(accurateCastOrDefault(same_width, $$Enum8('good' = 1, 'fallback' = 2)$$, fallback)),
    isNull(accurateCastOrNull(same_width, $$Enum8('good' = 1, 'fallback' = 2)$$)),
    toInt8(accurateCastOrDefault(valid_source, $$Enum8('good' = 1, 'fallback' = 2)$$, fallback)),
    isNull(accurateCastOrNull(valid_source, $$Enum8('good' = 1, 'fallback' = 2)$$));
SELECT toInt8(accurateCast(toFloat64(1.5), $$Enum8('good' = 1, 'fallback' = 2)$$)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toInt8(accurateCast(CAST('large', $$Enum16('large' = 257)$$), $$Enum8('good' = 1, 'fallback' = 2)$$)); -- { serverError CANNOT_CONVERT_TYPE }

-- Ordinary CAST keeps its existing integer-conversion behavior.
SELECT toInt8(CAST(CAST('large', $$Enum16('large' = 257)$$), $$Enum8('good' = 1, 'fallback' = 2)$$));

SET check_conversion_from_numbers_to_enum = 1;
WITH
    CAST('large', $$Enum16('large' = 257)$$) AS narrowing,
    CAST('other', $$Enum8('other' = 7)$$) AS same_width,
    CAST('good', $$Enum16('good' = 1)$$) AS valid_source,
    CAST('fallback', $$Enum8('good' = 1, 'fallback' = 2)$$) AS fallback
SELECT
    toInt8(accurateCastOrDefault(narrowing, $$Enum8('good' = 1, 'fallback' = 2)$$, fallback)),
    isNull(accurateCastOrNull(narrowing, $$Enum8('good' = 1, 'fallback' = 2)$$)),
    toInt8(accurateCastOrDefault(same_width, $$Enum8('good' = 1, 'fallback' = 2)$$, fallback)),
    isNull(accurateCastOrNull(same_width, $$Enum8('good' = 1, 'fallback' = 2)$$)),
    toInt8(accurateCastOrDefault(valid_source, $$Enum8('good' = 1, 'fallback' = 2)$$, fallback)),
    isNull(accurateCastOrNull(valid_source, $$Enum8('good' = 1, 'fallback' = 2)$$));
SELECT toInt8(accurateCast(toFloat64(1.5), $$Enum8('good' = 1, 'fallback' = 2)$$)); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toInt8(accurateCast(CAST('large', $$Enum16('large' = 257)$$), $$Enum8('good' = 1, 'fallback' = 2)$$)); -- { serverError CANNOT_CONVERT_TYPE }
