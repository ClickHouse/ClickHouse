-- The DOM path (JSONExtract, typed JSON) must parse a fractional Unix timestamp for DateTime64
-- from its decimal text, preserving sub-second precision like the row input serializer,
-- CAST and toDateTime64, instead of converting through Float64 arithmetic. The parity is limited
-- to Float64 precision, because the JSON DOM stores every fractional number as a Float64.
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

-- The same applies to an oversized integer number: the integer path must fail softly like the
-- fractional path, rather than throwing DECIMAL_OVERFLOW. 9223372036854775808 = 2^63 is read as a
-- UInt64 and overflows the DateTime64 range once scaled. JSONExtract yields the default value, while
-- the typed JSON path reports a clean INCORRECT_DATA parse error (not an internal arithmetic overflow).
SELECT JSONExtract('{"t":9223372036854775808}', 't', 'DateTime64(3)');
SELECT CAST('{"t":9223372036854775808}', 'JSON(t DateTime64(3))'); -- { serverError INCORRECT_DATA }

-- The compatibility setting only affects integers (raw ticks); a fractional number is still seconds.
SELECT JSONExtract('{"t":0.58}', 't', 'DateTime64(2)') SETTINGS input_format_read_datetime_number_as_raw_value = 1;

-- In compatibility mode an integer is the raw scaled value (ticks) stored directly in the Int64 native
-- type: 1703363853035 ticks at scale 3 is 2023-12-23 20:37:33.035. A value beyond Int64 (2^63) is out of
-- range and must fail like the seconds path above -- JSONExtract yields the default value and the typed
-- JSON path reports a clean INCORRECT_DATA error -- rather than narrowing to a negative timestamp.
SELECT JSONExtract('{"t":1703363853035}', 't', 'DateTime64(3)') SETTINGS input_format_read_datetime_number_as_raw_value = 1;
SELECT JSONExtract('{"t":9223372036854775808}', 't', 'DateTime64(3)') SETTINGS input_format_read_datetime_number_as_raw_value = 1;
SELECT CAST('{"t":9223372036854775808}', 'JSON(t DateTime64(3))') SETTINGS input_format_read_datetime_number_as_raw_value = 1; -- { serverError INCORRECT_DATA }

-- Parity with the row input path holds only up to Float64 precision: the DOM parser has already
-- rounded the literal to the nearest Float64, so 1703363853.9999999 reaches the conversion as
-- 1703363854.0 and truncates to the next second, while the row input path truncates the original
-- text to 1703363853 (and .999 at scale 3).
SELECT JSONExtract('{"t":1703363853.9999999}', 't', 'DateTime');
SELECT t FROM format(JSONEachRow, 't DateTime', '{"t":1703363853.9999999}');
SELECT JSONExtract('{"t":1703363853.9999999}', 't', 'DateTime64(3)');
SELECT t FROM format(JSONEachRow, 't DateTime64(3)', '{"t":1703363853.9999999}');

-- A scientific-notation number that does not fit the decimal reader's precision (1e39 expands to 40
-- digits, beyond the reader's 38-digit precision) is rejected on the DOM path, exactly as the row input
-- serializer rejects it, rather than being silently clamped to the DateTime maximum. JSONExtract yields
-- the default value, the typed JSON path reports a clean INCORRECT_DATA error, and the row input path
-- fails to parse the number.
SELECT JSONExtract('{"t":1e39}', 't', 'DateTime');
SELECT CAST('{"t":1e39}', 'JSON(t DateTime)'); -- { serverError INCORRECT_DATA }
SELECT t FROM format(JSONEachRow, 't DateTime', '{"t":1e39}'); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- A large but in-precision number is clamped to the DateTime range on both the DOM and the row input
-- path, so the two agree.
SELECT JSONExtract('{"t":1e30}', 't', 'DateTime');
SELECT t FROM format(JSONEachRow, 't DateTime', '{"t":1e30}');

-- A negative (pre-epoch) number is a Unix timestamp below the DateTime range; it is clamped to the epoch
-- on the DOM path (JSONExtract, typed JSON) exactly as the row input serializer does, rather than being
-- rejected. This holds for both an integer and a fractional negative number.
SELECT JSONExtract('{"t":-1}', 't', 'DateTime');
SELECT JSONExtract('{"t":-0.5}', 't', 'DateTime');
SELECT CAST('{"t":-1}', 'JSON(t DateTime)');
SELECT CAST('{"t":-0.5}', 'JSON(t DateTime)');
SELECT JSONExtract('{"t":-1}', 't', 'DateTime') = (SELECT t FROM format(JSONEachRow, 't DateTime', '{"t":-1}'));
SELECT JSONExtract('{"t":-0.5}', 't', 'DateTime') = (SELECT t FROM format(JSONEachRow, 't DateTime', '{"t":-0.5}'));
