SELECT '-- const date, const delta';

SELECT '   -- add';
SELECT addYears('2023-10-22', 1), addYears('2023-10-22 12:34:56.123', 1);
SELECT addQuarters('2023-10-22', 1), addQuarters('2023-10-22 12:34:56.123', 1);
SELECT addMonths('2023-10-22', 1), addMonths('2023-10-22 12:34:56.123', 1);
SELECT addWeeks('2023-10-22', 1), addWeeks('2023-10-22 12:34:56.123', 1);
SELECT addDays('2023-10-22', 1), addDays('2023-10-22 12:34:56.123', 1);
SELECT addHours('2023-10-22', 1), addHours('2023-10-22 12:34:56.123', 1);
SELECT addMinutes('2023-10-22', 1), addMinutes('2023-10-22 12:34:56.123', 1);
SELECT addSeconds('2023-10-22', 1), addSeconds('2023-10-22 12:34:56.123', 1);
SELECT addMilliseconds('2023-10-22', 1), addMilliseconds('2023-10-22 12:34:56.123', 1);
SELECT addMicroseconds('2023-10-22', 1), addMicroseconds('2023-10-22 12:34:56.123', 1);
SELECT addNanoseconds('2023-10-22', 1), addNanoseconds('2023-10-22 12:34:56.123', 1);

SELECT '   -- subtract';
SELECT subtractYears('2023-10-22', 1), subtractYears('2023-10-22 12:34:56.123', 1);
SELECT subtractQuarters('2023-10-22', 1), subtractQuarters('2023-10-22 12:34:56.123', 1);
SELECT subtractMonths('2023-10-22', 1), subtractMonths('2023-10-22 12:34:56.123', 1);
SELECT subtractWeeks('2023-10-22', 1), subtractWeeks('2023-10-22 12:34:56.123', 1);
SELECT subtractDays('2023-10-22', 1), subtractDays('2023-10-22 12:34:56.123', 1);
SELECT subtractHours('2023-10-22', 1), subtractHours('2023-10-22 12:34:56.123', 1);
SELECT subtractMinutes('2023-10-22', 1), subtractMinutes('2023-10-22 12:34:56.123', 1);
SELECT subtractSeconds('2023-10-22', 1), subtractSeconds('2023-10-22 12:34:56.123', 1);
SELECT subtractMilliseconds('2023-10-22', 1), subtractMilliseconds('2023-10-22 12:34:56.123', 1);
SELECT subtractMicroseconds('2023-10-22', 1), subtractMicroseconds('2023-10-22 12:34:56.123', 1);
SELECT subtractNanoseconds('2023-10-22', 1), subtractNanoseconds('2023-10-22 12:34:56.123', 1);

SELECT '-- non-const date, const delta';

SELECT '   -- add';
SELECT addYears(materialize('2023-10-22'), 1), addYears(materialize('2023-10-22 12:34:56.123'), 1);
SELECT addQuarters(materialize('2023-10-22'), 1), addQuarters(materialize('2023-10-22 12:34:56.123'), 1);
SELECT addMonths(materialize('2023-10-22'), 1), addMonths(materialize('2023-10-22 12:34:56.123'), 1);
SELECT addWeeks(materialize('2023-10-22'), 1), addWeeks(materialize('2023-10-22 12:34:56.123'), 1);
SELECT addDays(materialize('2023-10-22'), 1), addDays(materialize('2023-10-22 12:34:56.123'), 1);
SELECT addHours(materialize('2023-10-22'), 1), addHours(materialize('2023-10-22 12:34:56.123'), 1);
SELECT addMinutes(materialize('2023-10-22'), 1), addMinutes(materialize('2023-10-22 12:34:56.123'), 1);
SELECT addSeconds(materialize('2023-10-22'), 1), addSeconds(materialize('2023-10-22 12:34:56.123'), 1);
SELECT addMilliseconds(materialize('2023-10-22'), 1), addMilliseconds(materialize('2023-10-22 12:34:56.123'), 1);
SELECT addMicroseconds(materialize('2023-10-22'), 1), addMicroseconds(materialize('2023-10-22 12:34:56.123'), 1);
SELECT addNanoseconds(materialize('2023-10-22'), 1), addNanoseconds(materialize('2023-10-22 12:34:56.123'), 1);

SELECT '   -- subtract';
SELECT subtractYears(materialize('2023-10-22'), 1), subtractYears(materialize('2023-10-22 12:34:56.123'), 1);
SELECT subtractQuarters(materialize('2023-10-22'), 1), subtractQuarters(materialize('2023-10-22 12:34:56.123'), 1);
SELECT subtractMonths(materialize('2023-10-22'), 1), subtractMonths(materialize('2023-10-22 12:34:56.123'), 1);
SELECT subtractWeeks(materialize('2023-10-22'), 1), subtractWeeks(materialize('2023-10-22 12:34:56.123'), 1);
SELECT subtractDays(materialize('2023-10-22'), 1), subtractDays(materialize('2023-10-22 12:34:56.123'), 1);
SELECT subtractHours(materialize('2023-10-22'), 1), subtractHours(materialize('2023-10-22 12:34:56.123'), 1);
SELECT subtractMinutes(materialize('2023-10-22'), 1), subtractMinutes(materialize('2023-10-22 12:34:56.123'), 1);
SELECT subtractSeconds(materialize('2023-10-22'), 1), subtractSeconds(materialize('2023-10-22 12:34:56.123'), 1);
SELECT subtractMilliseconds(materialize('2023-10-22'), 1), subtractMilliseconds(materialize('2023-10-22 12:34:56.123'), 1);
SELECT subtractMicroseconds(materialize('2023-10-22'), 1), subtractMicroseconds(materialize('2023-10-22 12:34:56.123'), 1);
SELECT subtractNanoseconds(materialize('2023-10-22'), 1), subtractNanoseconds(materialize('2023-10-22 12:34:56.123'), 1);

SELECT '-- const date, non-const delta';

SELECT '   -- add';
SELECT addYears('2023-10-22', materialize(1)), addYears('2023-10-22 12:34:56.123', materialize(1));
SELECT addQuarters('2023-10-22', materialize(1)), addQuarters('2023-10-22 12:34:56.123', materialize(1));
SELECT addMonths('2023-10-22', materialize(1)), addMonths('2023-10-22 12:34:56.123', materialize(1));
SELECT addWeeks('2023-10-22', materialize(1)), addWeeks('2023-10-22 12:34:56.123', materialize(1));
SELECT addDays('2023-10-22', materialize(1)), addDays('2023-10-22 12:34:56.123', materialize(1));
SELECT addHours('2023-10-22', materialize(1)), addHours('2023-10-22 12:34:56.123', materialize(1));
SELECT addMinutes('2023-10-22', materialize(1)), addMinutes('2023-10-22 12:34:56.123', materialize(1));
SELECT addSeconds('2023-10-22', materialize(1)), addSeconds('2023-10-22 12:34:56.123', materialize(1));
SELECT addMilliseconds('2023-10-22', materialize(1)), addMilliseconds('2023-10-22 12:34:56.123', materialize(1));
SELECT addMicroseconds('2023-10-22', materialize(1)), addMicroseconds('2023-10-22 12:34:56.123', materialize(1));
SELECT addNanoseconds('2023-10-22', materialize(1)), addNanoseconds('2023-10-22 12:34:56.123', materialize(1));

SELECT '   -- subtract';
SELECT subtractYears('2023-10-22', materialize(1)), subtractYears('2023-10-22 12:34:56.123', materialize(1));
SELECT subtractQuarters('2023-10-22', materialize(1)), subtractQuarters('2023-10-22 12:34:56.123', materialize(1));
SELECT subtractMonths('2023-10-22', materialize(1)), subtractMonths('2023-10-22 12:34:56.123', materialize(1));
SELECT subtractWeeks('2023-10-22', materialize(1)), subtractWeeks('2023-10-22 12:34:56.123', materialize(1));
SELECT subtractDays('2023-10-22', materialize(1)), subtractDays('2023-10-22 12:34:56.123', materialize(1));
SELECT subtractHours('2023-10-22', materialize(1)), subtractHours('2023-10-22 12:34:56.123', materialize(1));
SELECT subtractMinutes('2023-10-22', materialize(1)), subtractMinutes('2023-10-22 12:34:56.123', materialize(1));
SELECT subtractSeconds('2023-10-22', materialize(1)), subtractSeconds('2023-10-22 12:34:56.123', materialize(1));
SELECT subtractMilliseconds('2023-10-22', materialize(1)), subtractMilliseconds('2023-10-22 12:34:56.123', materialize(1));
SELECT subtractMicroseconds('2023-10-22', materialize(1)), subtractMicroseconds('2023-10-22 12:34:56.123', materialize(1));
SELECT subtractNanoseconds('2023-10-22', materialize(1)), subtractNanoseconds('2023-10-22 12:34:56.123', materialize(1));

SELECT '-- non-const date, non-const delta';

SELECT '   -- add';
SELECT addYears(materialize('2023-10-22'), materialize(1)), addYears(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT addQuarters(materialize('2023-10-22'), materialize(1)), addQuarters(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT addMonths(materialize('2023-10-22'), materialize(1)), addMonths(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT addWeeks(materialize('2023-10-22'), materialize(1)), addWeeks(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT addDays(materialize('2023-10-22'), materialize(1)), addDays(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT addHours(materialize('2023-10-22'), materialize(1)), addHours(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT addMinutes(materialize('2023-10-22'), materialize(1)), addMinutes(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT addSeconds(materialize('2023-10-22'), materialize(1)), addSeconds(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT addMilliseconds(materialize('2023-10-22'), materialize(1)), addMilliseconds(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT addMicroseconds(materialize('2023-10-22'), materialize(1)), addMicroseconds(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT addNanoseconds(materialize('2023-10-22'), materialize(1)), addNanoseconds(materialize('2023-10-22 12:34:56.123'), materialize(1));

SELECT '   -- subtract';
SELECT subtractYears(materialize('2023-10-22'), materialize(1)), subtractYears(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT subtractQuarters(materialize('2023-10-22'), materialize(1)), subtractQuarters(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT subtractMonths(materialize('2023-10-22'), materialize(1)), subtractMonths(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT subtractWeeks(materialize('2023-10-22'), materialize(1)), subtractWeeks(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT subtractDays(materialize('2023-10-22'), materialize(1)), subtractDays(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT subtractHours(materialize('2023-10-22'), materialize(1)), subtractHours(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT subtractMinutes(materialize('2023-10-22'), materialize(1)), subtractMinutes(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT subtractSeconds(materialize('2023-10-22'), materialize(1)), subtractSeconds(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT subtractMilliseconds(materialize('2023-10-22'), materialize(1)), subtractMilliseconds(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT subtractMicroseconds(materialize('2023-10-22'), materialize(1)), subtractMicroseconds(materialize('2023-10-22 12:34:56.123'), materialize(1));
SELECT subtractNanoseconds(materialize('2023-10-22'), materialize(1)), subtractNanoseconds(materialize('2023-10-22 12:34:56.123'), 1);

SELECT '-- plus operator';

SELECT '2023-10-23' + INTERVAL 1 YEAR, '2023-10-23 12:34:56.123' + INTERVAL 1 YEAR;
SELECT '2023-10-23' + INTERVAL 1 QUARTER, '2023-10-23 12:34:56.123' + INTERVAL 1 QUARTER;
SELECT '2023-10-23' + INTERVAL 1 MONTH,'2023-10-23 12:34:56.123' + INTERVAL 1 MONTH;
SELECT '2023-10-23' + INTERVAL 1 WEEK, '2023-10-23 12:34:56.123' + INTERVAL 1 WEEK;
SELECT '2023-10-23' + INTERVAL 1 DAY, '2023-10-23 12:34:56.123' + INTERVAL 1 DAY;
SELECT '2023-10-23' + INTERVAL 1 HOUR, '2023-10-23 12:34:56.123' + INTERVAL 1 HOUR;
SELECT '2023-10-23' + INTERVAL 1 MINUTE, '2023-10-23 12:34:56.123' + INTERVAL 1 MINUTE;
SELECT '2023-10-23' + INTERVAL 1 SECOND, '2023-10-23 12:34:56.123' + INTERVAL 1 SECOND;
SELECT '2023-10-23' + INTERVAL 1 MILLISECOND, '2023-10-23 12:34:56.123' + INTERVAL 1 MILLISECOND;
SELECT '2023-10-23' + INTERVAL 1 MICROSECOND, '2023-10-23 12:34:56.123' + INTERVAL 1 MICROSECOND;
SELECT '2023-10-23' + INTERVAL 1 NANOSECOND, '2023-10-23 12:34:56.123' + INTERVAL 1 NANOSECOND;

SELECT '-- minus operator';

SELECT '2023-10-23' - INTERVAL 1 YEAR, '2023-10-23 12:34:56.123' - INTERVAL 1 YEAR;
SELECT '2023-10-23' - INTERVAL 1 QUARTER, '2023-10-23 12:34:56.123' - INTERVAL 1 QUARTER;
SELECT '2023-10-23' - INTERVAL 1 MONTH, '2023-10-23 12:34:56.123' - INTERVAL 1 MONTH;
SELECT '2023-10-23' - INTERVAL 1 WEEK, '2023-10-23 12:34:56.123' - INTERVAL 1 WEEK;
SELECT '2023-10-23' - INTERVAL 1 DAY, '2023-10-23 12:34:56.123' - INTERVAL 1 DAY;
SELECT '2023-10-23' - INTERVAL 1 HOUR, '2023-10-23 12:34:56.123' - INTERVAL 1 HOUR;
SELECT '2023-10-23' - INTERVAL 1 MINUTE, '2023-10-23 12:34:56.123' - INTERVAL 1 MINUTE;
SELECT '2023-10-23' - INTERVAL 1 SECOND, '2023-10-23 12:34:56.123' - INTERVAL 1 SECOND;
SELECT '2023-10-23' - INTERVAL 1 MILLISECOND, '2023-10-23 12:34:56.123' - INTERVAL 1 MILLISECOND;
SELECT '2023-10-23' - INTERVAL 1 MICROSECOND, '2023-10-23 12:34:56.123' - INTERVAL 1 MICROSECOND;
SELECT '2023-10-23' - INTERVAL 1 NANOSECOND, '2023-10-23 12:34:56.123' - INTERVAL 1 NANOSECOND;
