-- Extracting a string JSON value into DateTime/DateTime64 is a string-to-type cast, so it
-- must honour cast_string_to_date_time_mode rather than date_time_input_format.
-- https://github.com/ClickHouse/ClickHouse/issues/109126

SET cast_string_to_date_time_mode = 'best_effort', date_time_input_format = 'basic';
SELECT JSONExtract('{"date":"2020-01-01 00:00:00.123Z"}', 'date', 'DateTime64(3, ''UTC'')');
SELECT JSONExtract('{"date":"2020-01-01T00:00:00Z"}', 'date', 'DateTime(''UTC'')');

-- date_time_input_format must not affect JSONExtract
SET cast_string_to_date_time_mode = 'basic', date_time_input_format = 'best_effort';
SELECT JSONExtract('{"date":"2020-01-01 00:00:00.123Z"}', 'date', 'DateTime64(3, ''UTC'')');

SET cast_string_to_date_time_mode = 'best_effort_us';
SELECT JSONExtract('{"date":"01/02/2020 00:00:00"}', 'date', 'DateTime(''UTC'')');
