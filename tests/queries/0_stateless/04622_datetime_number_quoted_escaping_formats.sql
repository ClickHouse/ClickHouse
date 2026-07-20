-- The 26.7 change of reading an unquoted number for a `DateTime`/`DateTime64` column as a Unix
-- timestamp in seconds applies to every input path that parses fields with the `Quoted` escaping
-- rule, not only to the `Values` format: `MySQLDump` and the `Template`/`CustomSeparated`/`Regexp`
-- formats configured with `Quoted` field escaping route through the same quoted serializer.
-- The `input_format_read_datetime_number_as_raw_value` rollback knob works there as well.
-- https://github.com/ClickHouse/ClickHouse/pull/108091

SET session_timezone = 'UTC';

SELECT '-- MySQLDump: an unquoted number for DateTime64 is a Unix timestamp in seconds';
SELECT t FROM format(MySQLDump, 't DateTime64(3)', 'INSERT INTO test VALUES (1703363853), (1703363853.035);');

SELECT '-- MySQLDump: the same for DateTime, a fractional number is truncated to whole seconds';
SELECT t FROM format(MySQLDump, 't DateTime', 'INSERT INTO test VALUES (1703363853), (1703363853.7);');

SELECT '-- MySQLDump compatibility: a bare integer for DateTime64 is the raw scaled value (ticks)';
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT t FROM format(MySQLDump, 't DateTime64(3)', 'INSERT INTO test VALUES (1703363853035);');

SELECT '-- MySQLDump compatibility: a fractional number is rejected';
SELECT t FROM format(MySQLDump, 't DateTime64(3)', 'INSERT INTO test VALUES (1703363853.035);'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SELECT t FROM format(MySQLDump, 't DateTime', 'INSERT INTO test VALUES (1703363853.7);'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
SET input_format_read_datetime_number_as_raw_value = 0;

SELECT '-- CustomSeparated with Quoted escaping: an unquoted number for DateTime64 is seconds';
SELECT t FROM format(CustomSeparated, 't DateTime64(3)', '1703363853\n1703363853.035\n')
SETTINGS format_custom_escaping_rule = 'Quoted';

SELECT '-- CustomSeparated with Quoted escaping: the same for DateTime';
SELECT t FROM format(CustomSeparated, 't DateTime', '1703363853\n1703363853.7\n')
SETTINGS format_custom_escaping_rule = 'Quoted';

SELECT '-- CustomSeparated compatibility: a bare integer for DateTime64 is the raw scaled value (ticks)';
SELECT t FROM format(CustomSeparated, 't DateTime64(3)', '1703363853035\n')
SETTINGS format_custom_escaping_rule = 'Quoted', input_format_read_datetime_number_as_raw_value = 1;

SELECT '-- CustomSeparated compatibility: a fractional number is rejected';
SELECT t FROM format(CustomSeparated, 't DateTime64(3)', '1703363853.035\n')
SETTINGS format_custom_escaping_rule = 'Quoted', input_format_read_datetime_number_as_raw_value = 1; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

SELECT '-- Quoted strings are unaffected by the setting in both formats';
SELECT t FROM format(MySQLDump, 't DateTime64(3)', 'INSERT INTO test VALUES (''2023-12-23 20:37:33.035'');');
SELECT t FROM format(CustomSeparated, 't DateTime64(3)', '''2023-12-23 20:37:33.035''\n')
SETTINGS format_custom_escaping_rule = 'Quoted';
SET input_format_read_datetime_number_as_raw_value = 1;
SELECT t FROM format(MySQLDump, 't DateTime64(3)', 'INSERT INTO test VALUES (''2023-12-23 20:37:33.035'');');
SELECT t FROM format(CustomSeparated, 't DateTime64(3)', '''2023-12-23 20:37:33.035''\n')
SETTINGS format_custom_escaping_rule = 'Quoted';
SET input_format_read_datetime_number_as_raw_value = 0;
