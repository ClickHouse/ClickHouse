-- Test the try-deserialization path (Variant column) for an unquoted DateTime/DateTime64 number,
-- for both the default (seconds) reading and the compatibility raw-value (ticks) reading.
SET session_timezone = 'UTC';
SET allow_experimental_variant_type = 1;
SET input_format_read_datetime_number_as_raw_value = 0;

SELECT '-- default: Variant try-path reads an unquoted number as a Unix timestamp in seconds';
SELECT v FROM format(JSONEachRow, 'v Variant(String, DateTime64(3))', '{"v":1703363853.035}') ORDER BY toString(v);
SELECT v FROM format(Values, 'v Variant(String, DateTime64(3))', '(1703363853.035)') ORDER BY toString(v);
SELECT v FROM format(JSONEachRow, 'v Variant(String, DateTime)', '{"v":1703363853.7}') ORDER BY toString(v);
SELECT v FROM format(Values, 'v Variant(String, DateTime)', '(1703363853)') ORDER BY toString(v);

SELECT '-- compatibility: Variant try-path reads a bare integer as the raw value (ticks for DateTime64)';
SELECT v FROM format(JSONEachRow, 'v Variant(String, DateTime64(3))', '{"v":1703363853035}') ORDER BY toString(v) SETTINGS input_format_read_datetime_number_as_raw_value = 1;
SELECT v FROM format(Values, 'v Variant(String, DateTime64(3))', '(1703363853035)') ORDER BY toString(v) SETTINGS input_format_read_datetime_number_as_raw_value = 1;
SELECT v FROM format(JSONEachRow, 'v Variant(String, DateTime)', '{"v":1703363853}') ORDER BY toString(v) SETTINGS input_format_read_datetime_number_as_raw_value = 1;
SELECT v FROM format(Values, 'v Variant(String, DateTime)', '(1703363853)') ORDER BY toString(v) SETTINGS input_format_read_datetime_number_as_raw_value = 1;
