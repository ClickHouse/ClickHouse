-- In the basic `DateTime64` text format a fractional separator must be followed by at least one digit.
-- A bare '.' used to be accepted as zero subseconds.
-- https://github.com/ClickHouse/ClickHouse/pull/86431

SET date_time_input_format = 'basic', cast_string_to_date_time_mode = 'basic';

SELECT toDateTime64('2025-01-02 03:04:05.', 3, 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toDateTime64('1234.', 3, 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toDateTime64('12345.', 3, 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toDateTime64('.', 3, 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT CAST('2025-01-02 03:04:05.' AS DateTime64(3, 'UTC')); -- { serverError CANNOT_PARSE_DATETIME }

-- A dot followed by another character is left unread and reported as trailing characters.
SELECT toDateTime64('2025-01-02 03:04:05.x', 3, 'UTC'); -- { serverError CANNOT_PARSE_TEXT }
SELECT toDateTime64(CAST('2025-01-02 03:04:05.' AS FixedString(25)), 3, 'UTC'); -- { serverError CANNOT_PARSE_TEXT, CANNOT_PARSE_DATETIME }

SELECT toDateTime64OrNull('2025-01-02 03:04:05.', 3, 'UTC');
SELECT toDateTime64OrNull('1234.', 3, 'UTC');
SELECT toDateTime64OrNull('2025-01-02 03:04:05.x', 3, 'UTC');

-- Values with fractional digits, and without a fractional part, are still accepted.
SELECT toDateTime64('2025-01-02 03:04:05.5', 3, 'UTC');
SELECT toDateTime64('1234.5', 3, 'UTC');
SELECT toDateTime64('.5', 3, 'UTC');
SELECT toDateTime64('2025-01-02 03:04:05', 3, 'UTC');
SELECT toDateTime64OrNull('2025-01-02 03:04:05.5', 3, 'UTC');
