-- Regression test for the `timestamp` function rejecting trailing characters after a DateTime value.
-- After reworking small-timestamp parsing, the DateTime parser stops at the first character that
-- cannot continue the value (e.g. a field delimiter of a row-based format), so the `timestamp`
-- function must reject any leftover characters instead of silently truncating a malformed argument
-- such as '2024 April 4' to the Unix timestamp 2024.
SET session_timezone = 'UTC';

-- Well-formed values are still accepted.
SELECT timestamp('2024-04-04');
SELECT timestamp('2024-04-04 12:00:00.123456');

-- Trailing characters after the value must be rejected.
SELECT timestamp('2024 April 4'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT timestamp('2024-04-04 12:00:00 extra'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT timestamp('12.5 abc'); -- { serverError CANNOT_PARSE_DATETIME }

-- FixedString: trailing zero bytes are padding and must be ignored, other trailing characters rejected.
SELECT timestamp(CAST('2024-04-04' AS FixedString(20)));
SELECT timestamp(CAST('2024 April 4' AS FixedString(12))); -- { serverError CANNOT_PARSE_DATETIME }

-- The optional second argument (a time added to the value) must reject trailing characters too.
SELECT timestamp('2024-04-04', '12:00:00');
SELECT timestamp('2024-04-04', '12:00:00 junk'); -- { serverError CANNOT_PARSE_DATETIME }
