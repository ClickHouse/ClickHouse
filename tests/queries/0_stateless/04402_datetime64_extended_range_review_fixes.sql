-- Review-comment fixes for the DateTime64 [0000, 9999] extension. All cases use UTC explicitly to stay
-- independent of the randomized session time zone.

SELECT '-- changeYear/changeMonth on DateTime64 cover [0000, 9999], not just [1900, 2299]';
SELECT changeYear(toDateTime64('2000-06-15 12:00:00', 3, 'UTC'), 1850),
       changeYear(toDateTime64('2000-06-15 12:00:00', 3, 'UTC'), 5000),
       changeYear(toDateTime64('2000-06-15 12:00:00', 3, 'UTC'), 9999),
       changeMonth(toDateTime64('0500-06-15 00:00:00', 3, 'UTC'), 12);
SELECT '-- scale 9 keeps working in-range and saturates gracefully out of its narrower tick range';
SELECT changeYear(toDateTime64('2000-01-01 00:00:00', 9, 'UTC'), 2100),
       toYear(changeYear(toDateTime64('2000-01-01 00:00:00', 9, 'UTC'), 2300)) >= 2261,
       toYear(changeYear(toDateTime64('2000-01-01 00:00:00', 9, 'UTC'), 1500)) <= 1678;

SELECT '-- parseDateTime64 century %C accepts [0, 99]';
SELECT parseDateTime64('18-03-04', '%C-%m-%d', 'UTC'),
       parseDateTime64('00-03-04', '%C-%m-%d', 'UTC'),
       parseDateTime64('99-03-04', '%C-%m-%d', 'UTC');

SELECT '-- addMonths saturates to the upper boundary instead of jumping back a year';
SELECT addMonths(toDateTime64('9999-12-31 00:00:00', 0, 'UTC'), 1),
       addMonths(toDateTime64('9999-11-15 00:00:00', 0, 'UTC'), 6);

SELECT '-- dateDiff hour/minute are correct across the 1900 boundary (match the 2250 analog)';
SELECT dateDiff('hour',   toDateTime64('1850-01-01 00:00:30', 0, 'UTC'), toDateTime64('1850-01-01 02:00:00', 0, 'UTC')) =
       dateDiff('hour',   toDateTime64('2250-01-01 00:00:30', 0, 'UTC'), toDateTime64('2250-01-01 02:00:00', 0, 'UTC')),
       dateDiff('minute', toDateTime64('1850-01-01 00:00:30', 0, 'UTC'), toDateTime64('1850-01-01 00:05:00', 0, 'UTC')) =
       dateDiff('minute', toDateTime64('2250-01-01 00:00:30', 0, 'UTC'), toDateTime64('2250-01-01 00:05:00', 0, 'UTC')),
       dateDiff('hour',   toDateTime64('1850-01-01 00:00:30', 0, 'UTC'), toDateTime64('1850-01-01 02:00:00', 0, 'UTC'));

SELECT '-- dateDiff week on out-of-range (pre-1900) dates uses floor: matches the in-range analog 400 years later';
SELECT dateDiff('week', toDateTime64('1850-03-11 00:00:00', 0, 'UTC'), toDateTime64('1850-06-03 00:00:00', 0, 'UTC')) =
       dateDiff('week', toDateTime64('2250-03-11 00:00:00', 0, 'UTC'), toDateTime64('2250-06-03 00:00:00', 0, 'UTC')),
       dateDiff('week', toDateTime64('1850-03-11 00:00:00', 0, 'UTC'), toDateTime64('1850-06-03 00:00:00', 0, 'UTC'));

SELECT '-- numeric toDateTime64 saturates per-scale instead of throwing DECIMAL_OVERFLOW (ticks are stored in Int64)';
-- The whole-seconds range shrinks with the scale: scale 8 tops out near year 4892 and scale 9 near 2262-04-11.
-- A value past the tick range must clamp (under the non-throwing overflow modes) rather than fail in DecimalUtils.
SELECT toDateTime64(300000000000, 9, 'UTC') = toDateTime64(9223372036, 9, 'UTC'),
       toDateTime64(300000000000, 8, 'UTC') = toDateTime64(92233720368, 8, 'UTC'),
       toDateTime64(-300000000000, 9, 'UTC') = toDateTime64(-9223372036, 9, 'UTC')
SETTINGS date_time_overflow_behavior = 'saturate';

SELECT '-- the float numeric path saturates per-scale too (it previously surfaced DECIMAL_OVERFLOW)';
SELECT toDateTime64(300000000000.0, 9, 'UTC') = toDateTime64(9223372036, 9, 'UTC'),
       toDateTime64(300000000000.0, 8, 'UTC') = toDateTime64(92233720368, 8, 'UTC'),
       toDateTime64(-300000000000.0, 9, 'UTC') = toDateTime64(-9223372036, 9, 'UTC')
SETTINGS date_time_overflow_behavior = 'saturate';

SELECT '-- scale 0 numeric conversion reaches the full [0000, 9999] range';
SELECT toYear(toDateTime64(253402300799, 0, 'UTC')) = 9999,
       toYear(toDateTime64(-62167219200, 0, 'UTC')) = 0;

-- date_time_overflow_behavior governs conversions between date/time types, not conversions from a raw number,
-- so numeric toDateTime64 saturates per-scale in every mode (including 'throw') instead of raising an error.
SELECT '-- numeric toDateTime64 saturates per-scale regardless of date_time_overflow_behavior';
SELECT toDateTime64(300000000000, 9, 'UTC') = toDateTime64(9223372036, 9, 'UTC'),
       toDateTime64(-300000000000, 9, 'UTC') = toDateTime64(-9223372036, 9, 'UTC')
SETTINGS date_time_overflow_behavior = 'throw';

SELECT '-- changeYear/changeMonth saturate at the partial boundary years instead of throwing DECIMAL_OVERFLOW';
-- At scale 9 only [1677-09-21, 2262-04-11] is representable and at scale 8 only up to ~4892-10-07, so a year that
-- passes the [min_year, max_year] check can still push the month/day past the Int64 tick range. changeDate must
-- saturate to the boundary; previously the multiply inside dateTimeFromComponents overflowed and raised
-- DECIMAL_OVERFLOW. Each case compares against an obviously-out-of-range target (year 9999 / 0) that saturates to the
-- same boundary, so the check does not depend on the exact boundary year.
SELECT changeYear(toDateTime64('2000-12-31 00:00:00', 9, 'UTC'), 2262) = changeYear(toDateTime64('2000-06-15 00:00:00', 9, 'UTC'), 9999),
       changeYear(toDateTime64('2000-01-01 00:00:00', 9, 'UTC'), 1677) = changeYear(toDateTime64('2000-06-15 00:00:00', 9, 'UTC'), 0),
       changeYear(toDateTime64('2000-12-31 00:00:00', 8, 'UTC'), 4892) = changeYear(toDateTime64('2000-06-15 00:00:00', 8, 'UTC'), 9999),
       toMonth(changeMonth(toDateTime64('2262-01-01 00:00:00', 9, 'UTC'), 12)) = 4;

SELECT '-- toDateTime64 from a UInt64 above Int64::max saturates to the maximum instead of wrapping to a pre-epoch value';
-- Regression: the non-throwing UInt64 path cast to a signed time_t before clamping, so 18446744073709551615 became -1
-- and produced a pre-epoch DateTime64. It must compare in the unsigned domain and saturate to the per-scale maximum.
SELECT toYear(toDateTime64(18446744073709551615, 9, 'UTC')) = 2262,
       toYear(toDateTime64(18446744073709551615, 0, 'UTC')) = 9999;

SELECT '-- Date32 -> DateTime64 saturates per-scale instead of throwing DECIMAL_OVERFLOW (previously overflowed at scale 9)';
-- Date32 reaches 2299-12-31 (10413705600 whole seconds). At scale 9 only [1677-09-21, 2262-04-11] is representable, so
-- the conversion must saturate to the boundary (same as the numeric scale-9 maximum) instead of overflowing the Int64
-- ticks in decimalFromComponents. At scale 8 the range tops out near year 4892, so the true 2299-12-31 is preserved.
SELECT CAST(toDate32('2299-12-31'), 'DateTime64(9, ''UTC'')') = toDateTime64(9223372036, 9, 'UTC'),
       CAST(toDate32('2299-12-31'), 'DateTime64(8, ''UTC'')') = toDateTime64(10413705600, 8, 'UTC'),
       toString(CAST(toDate32('2299-12-31'), 'DateTime64(8, ''UTC'')'));
