SET short_circuit_function_evaluation = 'enable';
SET cast_keep_nullable = 1;

-- Constants and full columns use the same accurate conversion.
SELECT _accurateCast('1', 'UInt8'), toTypeName(_accurateCast('1', 'UInt8'));
SELECT _accurateCast(toString(number), 'UInt8') FROM numbers(3);
SELECT _accurateCast(materialize(toInt16(-1)), 'UInt8'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT _accurateCast(materialize(toInt16(300)), 'UInt8'); -- { serverError CANNOT_CONVERT_TYPE }

-- The target type is exact regardless of source nullability and query settings.
SELECT _accurateCast(toNullable(toInt8(1)), 'UInt8'), toTypeName(_accurateCast(toNullable(toInt8(1)), 'UInt8'));
SELECT _accurateCast(NULL, 'Nullable(UInt8)'), toTypeName(_accurateCast(NULL, 'Nullable(UInt8)'));
SELECT _accurateCast(materialize(CAST(NULL, 'Nullable(UInt8)')), 'UInt16'); -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT _accurateCast(materialize('bad'), 'Nullable(UInt8)');

-- `LowCardinality` inputs retain their conversion semantics without converting unused dictionary entries.
SELECT _accurateCast(toLowCardinality(toString(number)), 'UInt8') FROM numbers(3);
SELECT _accurateCast(materialize('1'), 'LowCardinality(String)'), toTypeName(_accurateCast(materialize('1'), 'LowCardinality(String)'));

-- `Dynamic` and `Variant` inputs are converted as complete columns, including their null values.
SELECT _accurateCast(CAST(NULL, 'Dynamic'), 'UInt8'), toTypeName(_accurateCast(CAST(NULL, 'Dynamic'), 'UInt8'));
SELECT _accurateCast(CAST(toInt16(2), 'Dynamic'), 'UInt8');
SELECT _accurateCast(CAST(toInt16(3), 'Variant(Int16, String)'), 'UInt8');

-- Internal time conversions saturate at the target range and keep the requested time zone.
SELECT toUnixTimestamp(_accurateCast(toDateTime64('1969-12-31 23:59:59', 0, 'UTC'), 'DateTime(\'UTC\')'));
SELECT toUnixTimestamp(_accurateCast(toDateTime64('2106-02-07 06:28:17', 0, 'UTC'), 'DateTime(\'UTC\')'));
SELECT _accurateCast(toDate32('1969-12-31'), 'Date');
SELECT toTypeName(_accurateCast(toDateTime64('2026-01-01 00:00:00', 0, 'Asia/Tokyo'), 'DateTime'));

SET cast_ipv4_ipv6_default_on_conversion_error = 1;
SET input_format_ipv4_default_on_conversion_error = 1;
SET input_format_ipv6_default_on_conversion_error = 1;
SET cast_string_to_date_time_mode = 'best_effort';
SELECT _accurateCast(materialize('bad'), 'IPv4'); -- { serverError CANNOT_PARSE_IPV4 }
SELECT _accurateCast(materialize('bad'), 'IPv6'); -- { serverError CANNOT_PARSE_IPV6 }
SELECT _accurateCast(materialize('01 Jan 2026 00:00:00'), 'DateTime'); -- { serverError CANNOT_PARSE_TEXT }

-- Preparing an expression does not validate source conversions that are never executed.
CREATE TABLE internal_cast_arrays (x Array(UInt64)) ENGINE = Memory;
SELECT _accurateCast(x, 'UInt8') FROM internal_cast_arrays;
INSERT INTO internal_cast_arrays VALUES ([0]);
SELECT if(x[1] = 0, toUInt8(1), _accurateCast(x, 'UInt8')) FROM internal_cast_arrays;
SELECT _accurateCast(x, 'UInt8') FROM internal_cast_arrays; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE internal_cast_arrays;

-- The type argument is validated at the SQL boundary.
SELECT _accurateCast(1, 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT _accurateCast(1, toString(number)) FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
