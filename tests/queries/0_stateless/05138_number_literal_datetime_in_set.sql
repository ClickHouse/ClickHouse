-- A number literal on the right-hand side of `IN` is retargeted to the type of the left-hand side.
-- For a date or a date-time left-hand side the literal denotes a number of days or seconds, so it
-- has to be compared as the number it resolves to. Reading its original text as a date-time string
-- instead gives no match at all, or `NULL` for a `Nullable` left-hand side.
--
-- A number or a `Decimal` left-hand side keeps comparing against the exact text of the literal, so
-- `1.123456789012345679` does not match the `Decimal` it rounds to through Float64.

SET enable_analyzer = 1;

SELECT 'analyzer' AS t;
SELECT toDateTime(16) IN (materialize(toDateTime(0)), 16.) AS datetime;
SELECT toDateTime(16) IN (tuple(materialize(0), 0x1p4)) AS datetime_tuple;
SELECT toDate(16) IN (materialize(toDate(0)), 16.) AS date;
SELECT toDate32(16) IN (materialize(toDate32(0)), 16.) AS date32;
SELECT toDateTime64(16, 3) IN (materialize(toDateTime64(0, 3)), 16.) AS datetime64;
SELECT CAST(toDateTime(16), 'Nullable(DateTime)') IN (materialize(CAST(toDateTime(0), 'Nullable(DateTime)')), 16.) AS nullable_datetime;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') IN (materialize(CAST('0', 'Decimal128(18)')), 1.123456789012345679) AS decimal_keeps_original_text;
SELECT CAST('18446744073709551616', 'UInt128') IN (materialize(CAST('0', 'UInt128')), 18446744073709551616) AS wide_integer_keeps_original_text;

SET enable_analyzer = 0;

-- The `Decimal` case is not repeated here: the old analyzer rewrites a non-constant right-hand side
-- row-wise and reads the literal as Float64, so it rounds before the comparison.
SELECT 'old analyzer' AS t;
SELECT toDateTime(16) IN (materialize(toDateTime(0)), 16.) AS datetime;
SELECT toDateTime(16) IN (tuple(materialize(0), 0x1p4)) AS datetime_tuple;
SELECT toDate(16) IN (materialize(toDate(0)), 16.) AS date;
SELECT toDate32(16) IN (materialize(toDate32(0)), 16.) AS date32;
SELECT toDateTime64(16, 3) IN (materialize(toDateTime64(0, 3)), 16.) AS datetime64;
SELECT CAST(toDateTime(16), 'Nullable(DateTime)') IN (materialize(CAST(toDateTime(0), 'Nullable(DateTime)')), 16.) AS nullable_datetime;
SELECT CAST('18446744073709551616', 'UInt128') IN (materialize(CAST('0', 'UInt128')), 18446744073709551616) AS wide_integer_keeps_original_text;
