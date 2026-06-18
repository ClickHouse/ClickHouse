-- Validate that date_time_input_format='best_effort' and cast_string_to_date_time_mode='best_effort' are the defaults.
-- best_effort format can parse non-basic date strings like '2024 April 4'.

-- Verify best_effort parsing works by default (no explicit SETTINGS) for DateTime
SELECT toDateTime('2024 April 4', 'UTC');
SELECT toDateTime('Apr 15, 2020 10:30:00', 'UTC');
SELECT CAST('2024 April 4' AS DateTime('UTC'));
SELECT CAST('Apr 15, 2020 10:30:00' AS DateTime('UTC'));

-- Verify best_effort parsing works by default (no explicit SETTINGS) for DateTime64
SELECT toDateTime64('2024 April 4', 3, 'UTC');
SELECT toDateTime64('Apr 15, 2020 10:30:00.123', 3, 'UTC');
SELECT CAST('2024 April 4' AS DateTime64(3, 'UTC'));
SELECT CAST('Apr 15, 2020 10:30:00.123' AS DateTime64(3, 'UTC'));

-- Verify basic mode rejects these formats for DateTime
SELECT toDateTime('2024 April 4', 'UTC') SETTINGS cast_string_to_date_time_mode = 'basic'; -- { serverError CANNOT_PARSE_DATETIME }
SELECT CAST('2024 April 4' AS DateTime('UTC')) SETTINGS cast_string_to_date_time_mode = 'basic'; -- { serverError CANNOT_PARSE_DATETIME }

-- Verify basic mode rejects these formats for DateTime64
SELECT toDateTime64('2024 April 4', 3, 'UTC') SETTINGS cast_string_to_date_time_mode = 'basic'; -- { serverError CANNOT_PARSE_DATETIME }
SELECT CAST('2024 April 4' AS DateTime64(3, 'UTC')) SETTINGS cast_string_to_date_time_mode = 'basic'; -- { serverError CANNOT_PARSE_DATETIME }
