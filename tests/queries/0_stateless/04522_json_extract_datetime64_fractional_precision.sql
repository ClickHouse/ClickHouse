-- The DOM path (JSONExtract, typed JSON) must parse a fractional Unix timestamp for DateTime64
-- from its decimal text, preserving sub-second precision exactly like the row input serializer,
-- CAST and toDateTime64, instead of converting through Float64 arithmetic.
-- https://github.com/ClickHouse/ClickHouse/pull/108091

SET session_timezone = 'UTC';

-- Float64 math would truncate 0.58 * 100 = 57.999... to 57 ticks; the decimal text gives the exact 58.
SELECT JSONExtract('{"t":0.58}', 't', 'DateTime64(2)');
SELECT JSONExtract('{"t":1703363853.035}', 't', 'DateTime64(3)');

-- The DOM path agrees with the row input serializer and with a DateTime64 literal.
SELECT JSONExtract('{"t":1703363853.035}', 't', 'DateTime64(3)') = (SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":1703363853.035}'));
SELECT JSONExtract('{"t":1703363853.035}', 't', 'DateTime64(3)') = toDateTime64('2023-12-23 20:37:33.035', 3);

-- Negative (pre-epoch) fractional timestamps.
SELECT JSONExtract('{"t":-0.58}', 't', 'DateTime64(2)');

-- At scale 9 the precision is limited by what Float64 preserves (~17 significant digits),
-- but the decimal text of the double must not be shifted further by tick arithmetic.
SELECT JSONExtract('{"t":1703363853.123456789}', 't', 'DateTime64(9)');

-- The typed JSON path goes through the same conversion.
SELECT CAST('{"t":1703363853.035}', 'JSON(t DateTime64(3))');

-- A number that does not fit DateTime64 yields the default value instead of an exception.
SELECT JSONExtract('{"t":1e300}', 't', 'DateTime64(3)');

-- The compatibility setting only affects integers (raw ticks); a fractional number is still seconds.
SELECT JSONExtract('{"t":0.58}', 't', 'DateTime64(2)') SETTINGS input_format_read_datetime_number_as_raw_value = 1;
