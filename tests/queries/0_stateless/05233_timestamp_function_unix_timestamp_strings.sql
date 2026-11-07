-- The `timestamp` function is built on the basic `DateTime64` text parser, so, exactly like `toDateTime64`
-- in basic mode, it accepts a numeric string as a Unix timestamp with an optional fractional part.
-- Values of five or more digits have always been accepted; short values such as '1234' and '1234.5'
-- are accepted since small decimal timestamps are parsed correctly instead of being misread as a date.
SET session_timezone = 'UTC';

SELECT timestamp('1234');
SELECT timestamp('1234.5');
SELECT timestamp('12345');
SELECT timestamp('1234567890');
SELECT timestamp('1234567890.123456');
SELECT timestamp('.5');
SELECT timestamp('-0.5');
SELECT timestamp('-1.5');
SELECT timestamp('1234', '00:00:01');
SELECT timestamp(CAST('1234.5' AS FixedString(20)));

-- A numeric value must still be the whole argument.
SELECT timestamp('1234 5'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT timestamp('1234.5x'); -- { serverError CANNOT_PARSE_DATETIME }
