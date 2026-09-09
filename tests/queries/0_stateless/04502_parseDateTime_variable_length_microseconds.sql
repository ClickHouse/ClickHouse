-- Like MySQL's STR_TO_DATE, the MySQL-syntax specifier %f accepts between 1 and 6 fractional digits
-- and interprets them as left-aligned microseconds (a shorter fragment is right-padded with zeros).

-- parseDateTime64 keeps the fractional part (the result is always DateTime64(6)).
SELECT parseDateTime64('2025-01-04 23:00:00.1', '%Y-%m-%d %H:%i:%s.%f', 'UTC');
SELECT parseDateTime64('2025-01-04 23:00:00.12', '%Y-%m-%d %H:%i:%s.%f', 'UTC');
SELECT parseDateTime64('2025-01-04 23:00:00.123', '%Y-%m-%d %H:%i:%s.%f', 'UTC');
SELECT parseDateTime64('2025-01-04 23:00:00.1234', '%Y-%m-%d %H:%i:%s.%f', 'UTC');
SELECT parseDateTime64('2025-01-04 23:00:00.12345', '%Y-%m-%d %H:%i:%s.%f', 'UTC');
SELECT parseDateTime64('2025-01-04 23:00:00.123456', '%Y-%m-%d %H:%i:%s.%f', 'UTC');

-- parseDateTime returns DateTime (no sub-second precision), so it accepts and then discards the fractional digits.
SELECT parseDateTime('2025-01-04 23:00:00.1', '%Y-%m-%d %H:%i:%s.%f', 'UTC');
SELECT parseDateTime('2025-01-04 23:00:00.123456', '%Y-%m-%d %H:%i:%s.%f', 'UTC');

-- OrZero / OrNull variants succeed for a short fractional part.
SELECT parseDateTime64OrZero('2025-01-04 23:00:00.42', '%Y-%m-%d %H:%i:%s.%f', 'UTC');
SELECT parseDateTime64OrNull('2025-01-04 23:00:00.42', '%Y-%m-%d %H:%i:%s.%f', 'UTC');

-- More than 6 fractional digits is rejected because the trailing characters remain unparsed.
SELECT parseDateTime64('2025-01-04 23:00:00.1234567', '%Y-%m-%d %H:%i:%s.%f', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }
-- Zero fractional digits is rejected.
SELECT parseDateTime64('2025-01-04 23:00:00.', '%Y-%m-%d %H:%i:%s.%f', 'UTC'); -- { serverError CANNOT_PARSE_DATETIME }

-- OrZero / OrNull return the default value / NULL for the same invalid input.
SELECT parseDateTime64OrZero('2025-01-04 23:00:00.1234567', '%Y-%m-%d %H:%i:%s.%f', 'UTC');
SELECT parseDateTime64OrNull('2025-01-04 23:00:00.1234567', '%Y-%m-%d %H:%i:%s.%f', 'UTC');
