-- An unquoted JSON number for a DateTime64/DateTime column is a Unix timestamp (seconds since the
-- epoch) with optional sub-second precision, consistent with the Values format, CAST and toDateTime64.
-- https://github.com/ClickHouse/ClickHouse/issues/59443

SET session_timezone = 'UTC';

SELECT '-- DateTime64(3): unquoted float (the reported bug)';
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":1703363853.035}');

SELECT '-- DateTime64(3): an unquoted integer is seconds, like Values and CAST';
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":1703363853}');
SELECT t FROM format(Values, 't DateTime64(3)', '(1703363853)');
SELECT CAST(1703363853 AS DateTime64(3));

SELECT '-- DateTime64(3): JSONEachRow agrees with Values for a fractional timestamp';
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":1703363853.035}');
SELECT t FROM format(Values, 't DateTime64(3)', '(1703363853.035)');

SELECT '-- DateTime64(3): quoted forms still work';
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":"1703363853.035"}');
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":"2023-12-23 20:37:33.035"}');

SELECT '-- DateTime64: scales 0/6/9, sub-second precision preserved, extra digits truncated';
SELECT t FROM format(JSONEachRow, 't DateTime64(0)', '{"t":1703363853.9}');
SELECT t FROM format(JSONEachRow, 't DateTime64(6)', '{"t":1703363853.035123}');
SELECT t FROM format(JSONEachRow, 't DateTime64(9)', '{"t":1703363853.035123456}');
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":1703363853.035999}');

SELECT '-- DateTime64(3): exponent notation';
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":1.703363853035e9}');

SELECT '-- DateTime64(3): negative timestamp (before the epoch)';
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":-0.5}');

SELECT '-- DateTime64(0): the Int64 ticks boundary is stored exactly, without wraparound';
SELECT toInt64(t) FROM format(JSONEachRow, 't DateTime64(0)', '{"t":9223372036854775807}');
SELECT toInt64(t) FROM format(JSONEachRow, 't DateTime64(0)', '{"t":-9223372036854775808}');

SELECT '-- DateTime64: a value just outside the Int64 range is rejected, not silently wrapped';
SELECT t FROM format(JSONEachRow, 't DateTime64(0)', '{"t":9223372036854775808}'); -- { serverError DECIMAL_OVERFLOW }
SELECT t FROM format(JSONEachRow, 't DateTime64(0)', '{"t":-9223372036854775809}'); -- { serverError DECIMAL_OVERFLOW }
SELECT t FROM format(Values, 't DateTime64(0)', '(9223372036854775808)'); -- { serverError DECIMAL_OVERFLOW }

SELECT '-- DateTime64(3): the scale multiplication is range-checked too';
SELECT toUnixTimestamp64Milli(t) FROM format(JSONEachRow, 't DateTime64(3)', '{"t":9223372036854775.807}');
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":9223372036854775808}'); -- { serverError DECIMAL_OVERFLOW }

SELECT '-- DateTime64(3): a delimiter after the value is handled (more than one column)';
SELECT * FROM format(JSONEachRow, 't DateTime64(3), n Int32', '{"t":1703363853.035,"n":7}');

SELECT '-- DateTime64(3): try-path via Nullable and Variant';
SELECT t FROM format(JSONEachRow, 't Nullable(DateTime64(3))', '{"t":1703363853.035}');
SELECT v FROM format(JSONEachRow, 'v Variant(String, DateTime64(3))', '{"v":1703363853.035}');

SELECT '-- DateTime: unquoted float is truncated to whole seconds, like CAST';
SELECT t FROM format(JSONEachRow, 't DateTime', '{"t":1703363853.7}');
SELECT CAST(1703363853.7 AS DateTime);

SELECT '-- DateTime: an unquoted integer is unchanged';
SELECT t FROM format(JSONEachRow, 't DateTime', '{"t":1703363853}');

SELECT '-- DateTime: try-path via Nullable';
SELECT t FROM format(JSONEachRow, 't Nullable(DateTime)', '{"t":1703363853.7}');
