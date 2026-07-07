-- The `input_format_read_datetime_number_as_raw_value` compatibility setting restores the pre-26.7
-- behavior of reading an unquoted number for a `DateTime64` column as the raw scaled value (ticks).
-- It also gates the `JSONExtract` / typed `JSON` DOM path.
-- https://github.com/ClickHouse/ClickHouse/pull/108091

SET session_timezone = 'UTC';

-- The `compatibility` setting is checked first, while the input setting is still at its default:
-- `compatibility` does not override a setting the user has changed explicitly.

SELECT '-- Default (26.7+): an unquoted integer for DateTime64 is a Unix timestamp in seconds';
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":1703363853}');
SELECT t FROM format(Values, 't DateTime64(3)', '(1703363853)');

SELECT '-- Setting compatibility to 26.6 restores the pre-26.7 raw scaled value (ticks)';
SET compatibility = '26.6';
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":1703363853}');
SELECT t FROM format(Values, 't DateTime64(3)', '(1703363853)');
SET compatibility = '';

SELECT '-- The explicit input setting has the same effect as the compatibility rollback';
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":1703363853}');
SELECT t FROM format(Values, 't DateTime64(3)', '(1703363853)');
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
