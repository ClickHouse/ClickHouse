-- The `timestamp` function accepts 'yyyy-mm-dd[ hh:mm:ss[.mmmmmm]]', so a fractional separator
-- must be followed by digits. The text parsers pad a bare '.' with zeros, which must not make
-- such an argument silently valid.
SET session_timezone = 'UTC';

SELECT timestamp('2024-04-04 12:00:00.'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT timestamp('1234.'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT timestamp(CAST('2024-04-04 12:00:00.' AS FixedString(30))); -- { serverError CANNOT_PARSE_DATETIME }
SELECT timestamp('2024-04-04', '12:00:00.'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT timestamp('2024-04-04', '.'); -- { serverError CANNOT_PARSE_DATETIME }

-- Values with fractional digits are still accepted.
SELECT timestamp('2024-04-04 12:00:00.5');
SELECT timestamp('1234.5');
SELECT timestamp(CAST('2024-04-04 12:00:00.5' AS FixedString(30)));
SELECT timestamp('2024-04-04', '12:00:00.5');
