-- The `Regexp` format with the `Quoted` field escaping rule follows the same contract for an
-- unquoted `DateTime`/`DateTime64` number as `Values`/`MySQLDump`/`CustomSeparated`: by default a
-- number is a Unix timestamp in seconds, and in compatibility mode
-- (`input_format_read_datetime_number_as_raw_value = 1`) a fractional number is rejected rather
-- than silently truncated to its integer prefix — a capture group has no trailing delimiter, so
-- the format must check that the value consumed the whole matched field.
-- https://github.com/ClickHouse/ClickHouse/pull/108091

SET session_timezone = 'UTC';
SET format_regexp = '^(.+)$', format_regexp_escaping_rule = 'Quoted';

SELECT '-- Regexp with Quoted escaping: an unquoted number for DateTime64 is seconds';
SELECT t FROM format(Regexp, 't DateTime64(3)', '1703363853\n1703363853.035\n');

SELECT '-- Regexp with Quoted escaping: the same for DateTime, a fractional number is truncated';
SELECT t FROM format(Regexp, 't DateTime', '1703363853\n1703363853.7\n');

SELECT '-- Regexp compatibility: a bare integer for DateTime64 is the raw scaled value (ticks)';
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT t FROM format(Regexp, 't DateTime64(3)', '1703363853035\n');

SELECT '-- Regexp compatibility: a fractional number is rejected, not truncated to its integer prefix';
SELECT t FROM format(Regexp, 't DateTime64(3)', '1703363853.035\n'); -- { serverError INCORRECT_DATA }
SELECT t FROM format(Regexp, 't DateTime', '1703363853.7\n'); -- { serverError INCORRECT_DATA }
SET input_format_read_datetime_number_as_raw_value = 0;

SELECT '-- Quoted strings are unaffected by the setting';
SELECT t FROM format(Regexp, 't DateTime64(3)', '''2023-12-23 20:37:33.035''\n');
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT t FROM format(Regexp, 't DateTime64(3)', '''2023-12-23 20:37:33.035''\n');
SET input_format_read_datetime_number_as_raw_value = 0;
