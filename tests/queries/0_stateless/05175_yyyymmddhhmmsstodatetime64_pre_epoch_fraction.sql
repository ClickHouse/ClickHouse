-- `YYYYMMDDhhmmssToDateTime64` must not negate the fractional part for pre-epoch dates.
SET session_timezone = 'UTC';

SELECT YYYYMMDDhhmmssToDateTime64(19500601120000.25, 3, 'UTC');
SELECT YYYYMMDDhhmmssToDateTime64(19690101000000.5, 3, 'UTC');
SELECT YYYYMMDDhhmmssToDateTime64(19691231235958.25, 3, 'UTC');

SELECT YYYYMMDDhhmmssToDateTime64(19500601120000.25, 3, 'UTC') = parseDateTime64BestEffort('1950-06-01 12:00:00.250', 3, 'UTC');
SELECT YYYYMMDDhhmmssToDateTime64(19690101000000.5, 3, 'UTC') = parseDateTime64BestEffort('1969-01-01 00:00:00.500', 3, 'UTC');
SELECT YYYYMMDDhhmmssToDateTime64(19691231235958.25, 3, 'UTC') = parseDateTime64BestEffort('1969-12-31 23:59:58.250', 3, 'UTC');
