-- The `input_format_read_datetime_number_as_raw_value` compatibility setting restores the pre-26.8
-- behavior of reading an unquoted number for a `DateTime64` column as the raw scaled value (ticks).
-- It also gates the `JSONExtract` / typed `JSON` DOM path.
-- https://github.com/ClickHouse/ClickHouse/pull/108091

SET session_timezone = 'UTC';

-- The `compatibility` setting is checked first, while the input setting is still at its default:
-- `compatibility` does not override a setting the user has changed explicitly.

SELECT '-- Default (26.8+): an unquoted integer for DateTime64 is a Unix timestamp in seconds';
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":1703363853}');
SELECT t FROM format(Values, 't DateTime64(3)', '(1703363853)');

SELECT '-- Setting compatibility to 26.7 restores the pre-26.8 raw scaled value (ticks)';
SET compatibility = '26.7';
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":1703363853}');
SELECT t FROM format(Values, 't DateTime64(3)', '(1703363853)');
SET compatibility = '';

SELECT '-- The explicit input setting has the same effect as the compatibility rollback';
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":1703363853}');
SELECT t FROM format(Values, 't DateTime64(3)', '(1703363853)');
SET input_format_read_datetime_number_as_raw_value = 0;

SELECT '-- Values: a fractional number falls back to SQL expression evaluation (seconds) regardless of the setting, as before 26.8';
SELECT t FROM format(Values, 't DateTime64(3)', '(1703363853.035)');
SELECT t FROM format(Values, 't DateTime', '(1703363853.7)');
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT t FROM format(Values, 't DateTime64(3)', '(1703363853.035)');
SELECT t FROM format(Values, 't DateTime', '(1703363853.7)');
SET input_format_read_datetime_number_as_raw_value = 0;

SELECT '-- DateTime: an unquoted integer is a whole number of seconds either way';
SELECT t FROM format(JSONEachRow, 't DateTime', '{"t":1703363853}');
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT t FROM format(JSONEachRow, 't DateTime', '{"t":1703363853}');
SET input_format_read_datetime_number_as_raw_value = 0;

SELECT '-- DateTime: a fractional number is truncated by default, but rejected in compatibility mode';
SELECT t FROM format(JSONEachRow, 't DateTime', '{"t":1703363853.7}');
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT t FROM format(JSONEachRow, 't DateTime', '{"t":1703363853.7}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SET input_format_read_datetime_number_as_raw_value = 0;

SELECT '-- DateTime64: a fractional number is seconds by default, but rejected in compatibility mode';
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":1703363853.035}');
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":1703363853.035}'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SET input_format_read_datetime_number_as_raw_value = 0;

SELECT '-- JSONExtract: an unquoted integer for DateTime64 is seconds, consistent with a float';
SELECT JSONExtract('{"t":1703363853}', 't', 'DateTime64(3)');
SELECT JSONExtract('{"t":1703363853.035}', 't', 'DateTime64(3)');

SELECT '-- JSONExtract compatibility: an integer is the raw scaled value (ticks), a float stays seconds';
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT JSONExtract('{"t":1703363853}', 't', 'DateTime64(3)');
SELECT JSONExtract('{"t":1703363853.035}', 't', 'DateTime64(3)');
SET input_format_read_datetime_number_as_raw_value = 0;

SELECT '-- JSONExtract: DateTime accepts a fractional number, truncated to whole seconds';
SELECT JSONExtract('{"t":1703363853.7}', 't', 'DateTime');
SELECT JSONExtract('{"t":1703363853}', 't', 'DateTime');

SELECT '-- JSONExtract compatibility: a fractional DateTime number is rejected (returns the default)';
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT JSONExtract('{"t":1703363853.7}', 't', 'DateTime');
SELECT JSONExtract('{"t":1703363853}', 't', 'DateTime');
SET input_format_read_datetime_number_as_raw_value = 0;

SELECT '-- Typed JSON / JSONExtract: a negative integer for DateTime clamps to the epoch by default, but is rejected in compatibility mode';
SELECT JSONExtract('{"t":-1}', 't', 'Nullable(DateTime)');
SELECT CAST('{"t":-1}', 'JSON(t DateTime)');
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT JSONExtract('{"t":-1}', 't', 'Nullable(DateTime)');
SELECT CAST('{"t":-1}', 'JSON(t DateTime)'); -- { serverError INCORRECT_DATA }
SET input_format_read_datetime_number_as_raw_value = 0;

-- Boundary cases under the compatibility setting: the legacy integer path must not wrap around on
-- overflow, and the throwing (plain column) and non-throwing (Nullable) paths must agree.

SELECT '-- DateTime compatibility: an out-of-range integer clamps to the DateTime range (plain == Nullable)';
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT t FROM format(JSONEachRow, 't DateTime', '{"t":18446744073709551615}');
SELECT t FROM format(JSONEachRow, 't Nullable(DateTime)', '{"t":18446744073709551615}');
SET input_format_read_datetime_number_as_raw_value = 0;

SELECT '-- DateTime64 compatibility: a raw tick value beyond Int64 is out of range, not a wrapped negative';
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":9223372036854775808}'); -- { serverError DECIMAL_OVERFLOW }
SET input_format_read_datetime_number_as_raw_value = 0;

SELECT '-- DateTime compatibility: an integer wider than Int128 saturates and clamps instead of wrapping modulo 2^128';
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT t FROM format(JSONEachRow, 't DateTime', '{"t":340282366920938463463374607431768211456}');
SELECT t FROM format(JSONEachRow, 't Nullable(DateTime)', '{"t":340282366920938463463374607431768211456}');
SELECT t FROM format(JSONEachRow, 't DateTime', '{"t":-340282366920938463463374607431768211456}');
SET input_format_read_datetime_number_as_raw_value = 0;

SELECT '-- DateTime64 compatibility: an integer wider than Int128 is out of range instead of wrapping modulo 2^128';
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":340282366920938463463374607431768211456}'); -- { serverError DECIMAL_OVERFLOW }
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":340282366920938463463374609135132064491}'); -- { serverError DECIMAL_OVERFLOW }
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":-340282366920938463463374607431768211456}'); -- { serverError DECIMAL_OVERFLOW }
SET input_format_read_datetime_number_as_raw_value = 0;

SELECT '-- Compatibility mode still rejects a missing numeric token instead of loading the epoch';
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT t FROM format(JSONEachRow, 't DateTime', '{"t":}'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT t FROM format(JSONEachRow, 't Nullable(DateTime)', '{"t":}'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":}'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT t FROM format(JSONEachRow, 't Nullable(DateTime64(3))', '{"t":}'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT * FROM format(JSONEachRow, 't DateTime, u UInt8', '{"t":,"u":1}'); -- { serverError CANNOT_PARSE_NUMBER }
SELECT t FROM format(JSONEachRow, 't DateTime', '{"t":+}'); -- { serverError CANNOT_PARSE_NUMBER }
SET input_format_read_datetime_number_as_raw_value = 0;
