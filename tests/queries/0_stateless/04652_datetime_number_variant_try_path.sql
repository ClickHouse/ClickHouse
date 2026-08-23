-- Test the try-deserialization path (Variant column) for an unquoted DateTime/DateTime64 number,
-- for both the default (seconds) reading and the compatibility raw-value (ticks) reading.
SET session_timezone = 'UTC';
SET allow_experimental_variant_type = 1;
SET input_format_read_datetime_number_as_raw_value = 0;

SELECT '-- default: Variant try-path reads an unquoted number as a Unix timestamp in seconds';
SELECT v FROM format(JSONEachRow, 'v Variant(String, DateTime64(3))', '{"v":1703363853.035}') ORDER BY toString(v);
SELECT v FROM format(Values, 'v Variant(String, DateTime64(3))', '(1703363853.035)') ORDER BY toString(v);
SELECT v FROM format(JSONEachRow, 'v Variant(String, DateTime)', '{"v":1703363853.7}') ORDER BY toString(v);
SELECT v FROM format(Values, 'v Variant(String, DateTime)', '(1703363853.7)') ORDER BY toString(v);

SELECT '-- compatibility: DateTime64 bare integer is read as the raw value (ticks)';
SELECT v FROM format(JSONEachRow, 'v Variant(String, DateTime64(3))', '{"v":1703363853035}') ORDER BY toString(v) SETTINGS input_format_read_datetime_number_as_raw_value = 1;
SELECT v FROM format(Values, 'v Variant(String, DateTime64(3))', '(1703363853035)') ORDER BY toString(v) SETTINGS input_format_read_datetime_number_as_raw_value = 1;
-- Same fractional token as the default section above: the raw-value branch reads only the integer
-- part, leaves the '.7' unconsumed, so the DateTime try-parse fails and the Variant falls back to
-- String. This diverges from the default branch (2023-12-23 20:37:33), so it is a real regression check.
SELECT '-- compatibility: DateTime raw-value branch rejects a fractional token, so Variant falls back to String';
SELECT v FROM format(JSONEachRow, 'v Variant(String, DateTime)', '{"v":1703363853.7}') ORDER BY toString(v) SETTINGS input_format_read_datetime_number_as_raw_value = 1;
SELECT v FROM format(Values, 'v Variant(String, DateTime)', '(1703363853.7)') ORDER BY toString(v) SETTINGS input_format_read_datetime_number_as_raw_value = 1;
