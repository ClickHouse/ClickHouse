-- Test that JSON output of DateTime/DateTime64 respects date_time_output_format setting
-- When unix_timestamp format is used, the value should be a JSON number, not a quoted string

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
