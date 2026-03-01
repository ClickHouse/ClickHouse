-- Test that JSON output of DateTime/DateTime64 respects date_time_output_format setting
-- When unix_timestamp format is used, the value should be a JSON number, not a quoted string

-- Serialization tests
SELECT 'DateTime simple';
SELECT toDateTime('2020-10-15 10:00:00', 'UTC') AS ts FORMAT JSONEachRow SETTINGS date_time_output_format = 'simple';

SELECT 'DateTime unix_timestamp';
SELECT toDateTime('2020-10-15 10:00:00', 'UTC') AS ts FORMAT JSONEachRow SETTINGS date_time_output_format = 'unix_timestamp';

SELECT 'DateTime iso';
SELECT toDateTime('2020-10-15 10:00:00', 'UTC') AS ts FORMAT JSONEachRow SETTINGS date_time_output_format = 'iso';

SELECT 'DateTime64 simple';
SELECT toDateTime64('2020-10-15 10:00:00.123', 3, 'UTC') AS ts FORMAT JSONEachRow SETTINGS date_time_output_format = 'simple';

SELECT 'DateTime64 unix_timestamp';
SELECT toDateTime64('2020-10-15 10:00:00.123', 3, 'UTC') AS ts FORMAT JSONEachRow SETTINGS date_time_output_format = 'unix_timestamp';

SELECT 'DateTime64 iso';
SELECT toDateTime64('2020-10-15 10:00:00.123', 3, 'UTC') AS ts FORMAT JSONEachRow SETTINGS date_time_output_format = 'iso';

-- Roundtrip tests: serialize to JSON and parse back, verify the value is preserved
SELECT 'Roundtrip DateTime unix_timestamp';
SELECT * FROM format(JSONEachRow, 'ts DateTime(\'UTC\')', '{"ts":1602756000}') SETTINGS date_time_output_format = 'unix_timestamp' FORMAT JSONEachRow;

SELECT 'Roundtrip DateTime64 unix_timestamp';
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3, \'UTC\')', '{"ts":1602756000.123}') SETTINGS date_time_output_format = 'unix_timestamp' FORMAT JSONEachRow;

-- Roundtrip with quoted strings (simple/iso formats should still work)
SELECT 'Roundtrip DateTime simple';
SELECT * FROM format(JSONEachRow, 'ts DateTime(\'UTC\')', '{"ts":"2020-10-15 10:00:00"}') SETTINGS date_time_output_format = 'simple' FORMAT JSONEachRow;

SELECT 'Roundtrip DateTime64 simple';
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3, \'UTC\')', '{"ts":"2020-10-15 10:00:00.123"}') SETTINGS date_time_output_format = 'simple' FORMAT JSONEachRow;

-- Negative DateTime64 timestamps (before epoch)
SELECT 'DateTime64 negative unix_timestamp';
SELECT toDateTime64('1969-12-31 23:59:59.123', 3, 'UTC') AS ts FORMAT JSONEachRow SETTINGS date_time_output_format = 'unix_timestamp';

SELECT 'DateTime64 negative close to epoch';
SELECT toDateTime64('1969-12-31 23:59:59.877', 3, 'UTC') AS ts FORMAT JSONEachRow SETTINGS date_time_output_format = 'unix_timestamp';

-- DateTime64 with scale=0 (no fractional part)
SELECT 'DateTime64 scale0 unix_timestamp';
SELECT toDateTime64('2020-10-15 10:00:00', 0, 'UTC') AS ts FORMAT JSONEachRow SETTINGS date_time_output_format = 'unix_timestamp';
