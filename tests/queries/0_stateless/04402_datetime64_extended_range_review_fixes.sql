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
