SELECT 'Negative tests';
-- as changeYear, changeMonth, changeDay, changeMinute, changeSecond share the same implementation, just testing one of them
SELECT changeYear(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT changeYear(toDate('2000-01-01')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT changeYear(toDate('2000-01-01'), 2000, 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT changeYear(1999, 2000); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT changeYear(toDate('2000-01-01'), 'abc'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT changeYear(toDate('2000-01-01'), 1.5); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Disable timezone randomization
SET session_timezone='CET';

SELECT 'changeYear';
SELECT '-- Date';
SELECT changeYear(toDate('2000-01-01'), 2001);
SELECT changeYear(toDate('2000-01-01'), 1800); -- out-of-bounds
SELECT changeYear(toDate('2000-01-01'), -5000); -- out-of-bounds
SELECT changeYear(toDate('2000-01-01'), 2500); -- out-of-bounds
SELECT '-- Date32';
SELECT changeYear(toDate32('2000-01-01'), 2001);
SELECT changeYear(toDate32('2000-01-01'), 1800); -- out-of-bounds
SELECT changeYear(toDate32('2000-01-01'), -5000); -- out-of-bounds
SELECT changeYear(toDate32('2000-01-01'), 2500); -- out-of-bounds
SELECT '-- DateTime';
SELECT changeYear(toDateTime('2000-01-01 11:22:33'), 2001);
SELECT changeYear(toDateTime('2000-01-01 11:22:33'), 1800); -- out-of-bounds
SELECT changeYear(toDateTime('2000-01-01 11:22:33'), -5000); -- out-of-bounds
SELECT changeYear(toDateTime('2000-01-01 11:22:33'), 2500); -- out-of-bounds
SELECT '-- DateTime64';
SELECT changeYear(toDateTime64('2000-01-01 11:22:33.4444', 4), 2001);
SELECT changeYear(toDateTime64('2000-01-01 11:22:33.4444', 4), 1800); -- out-of-bounds
SELECT changeYear(toDateTime64('2000-01-01 11:22:33.4444', 4), -5000); -- out-of-bounds
SELECT changeYear(toDateTime64('2000-01-01 11:22:33.4444', 4), 2500); -- out-of-bounds

SELECT 'changeMonth';
SELECT '-- Date';
SELECT changeMonth(toDate('2000-01-01'), 1);
SELECT changeMonth(toDate('2000-01-01'), 2);
SELECT changeMonth(toDate('2000-01-01'), 12);
SELECT changeMonth(toDate('2000-01-01'), 0); -- out-of-bounds
SELECT changeMonth(toDate('2000-01-01'), -1); -- out-of-bounds
SELECT changeMonth(toDate('2000-01-01'), 13); -- out-of-bounds
SELECT '-- Date32';
SELECT changeMonth(toDate32('2000-01-01'), 1);
SELECT changeMonth(toDate32('2000-01-01'), 2);
SELECT changeMonth(toDate32('2000-01-01'), 12);
SELECT changeMonth(toDate32('2000-01-01'), 0); -- out-of-bounds
SELECT changeMonth(toDate32('2000-01-01'), -1); -- out-of-bounds
SELECT changeMonth(toDate32('2000-01-01'), 13); -- out-of-bounds
SELECT '-- DateTime';
SELECT changeMonth(toDateTime('2000-01-01 11:22:33'), 1);
SELECT changeMonth(toDateTime('2000-01-01 11:22:33'), 2);
SELECT changeMonth(toDateTime('2000-01-01 11:22:33'), 12);
SELECT changeMonth(toDateTime('2000-01-01 11:22:33'), 0); -- out-of-bounds
SELECT changeMonth(toDateTime('2000-01-01 11:22:33'), -1); -- out-of-bounds
SELECT changeMonth(toDateTime('2000-01-01 11:22:33'), 13); -- out-of-bounds
SELECT '-- DateTime64';
SELECT changeMonth(toDateTime64('2000-01-01 11:22:33.4444', 4), 1);
SELECT changeMonth(toDateTime64('2000-01-01 11:22:33.4444', 4), 2);
SELECT changeMonth(toDateTime64('2000-01-01 11:22:33.4444', 4), 12);
SELECT changeMonth(toDateTime64('2000-01-01 11:22:33.4444', 4), 0); -- out-of-bounds
SELECT changeMonth(toDateTime64('2000-01-01 11:22:33.4444', 4), -1); -- out-of-bounds
SELECT changeMonth(toDateTime64('2000-01-01 11:22:33.4444', 4), 13); -- out-of-bounds

SELECT 'changeDay';
SELECT '-- Date';
SELECT changeDay(toDate('2000-01-01'), 1);
SELECT changeDay(toDate('2000-01-01'), 2);
SELECT changeDay(toDate('2000-01-01'), 31);
SELECT changeDay(toDate('2000-01-01'), 0); -- out-of-bounds
SELECT changeDay(toDate('2000-01-01'), -1); -- out-of-bounds
SELECT changeDay(toDate('2000-01-01'), 32); -- out-of-bounds
SELECT '-- Date32';
SELECT changeDay(toDate32('2000-01-01'), 1);
SELECT changeDay(toDate32('2000-01-01'), 2);
SELECT changeDay(toDate32('2000-01-01'), 31);
SELECT changeDay(toDate32('2000-01-01'), 0); -- out-of-bounds
SELECT changeDay(toDate32('2000-01-01'), -1); -- out-of-bounds
SELECT changeDay(toDate32('2000-01-01'), 32); -- out-of-bounds
SELECT '-- DateTime';
SELECT changeDay(toDateTime('2000-01-01 11:22:33'), 1);
SELECT changeDay(toDateTime('2000-01-01 11:22:33'), 2);
SELECT changeDay(toDateTime('2000-01-01 11:22:33'), 31);
SELECT changeDay(toDateTime('2000-01-01 11:22:33'), 0); -- out-of-bounds
SELECT changeDay(toDateTime('2000-01-01 11:22:33'), -1); -- out-of-bounds
SELECT changeDay(toDateTime('2000-01-01 11:22:33'), 32); -- out-of-bounds
SELECT '-- DateTime64';
SELECT changeDay(toDateTime64('2000-01-01 11:22:33.4444', 4), 1);
SELECT changeDay(toDateTime64('2000-01-01 11:22:33.4444', 4), 2);
SELECT changeDay(toDateTime64('2000-01-01 11:22:33.4444', 4), 31);
SELECT changeDay(toDateTime64('2000-01-01 11:22:33.4444', 4), 0); -- out-of-bounds
SELECT changeDay(toDateTime64('2000-01-01 11:22:33.4444', 4), -1); -- out-of-bounds
SELECT changeDay(toDateTime64('2000-01-01 11:22:33.4444', 4), 32); -- out-of-bounds
SELECT '-- Special case: change to 29 Feb in a leap year';
SELECT changeDay(toDate('2000-02-28'), 29);
SELECT changeDay(toDate32('2000-02-01'), 29);
SELECT changeDay(toDateTime('2000-02-01 11:22:33'), 29);
SELECT changeDay(toDateTime64('2000-02-01 11:22:33.4444', 4), 29);

SELECT 'changeHour';
SELECT '-- Date';
SELECT changeHour(toDate('2000-01-01'), 0);
SELECT changeHour(toDate('2000-01-01'), 2);
SELECT changeHour(toDate('2000-01-01'), 23);
SELECT changeHour(toDate('2000-01-01'), -1); -- out-of-bounds
SELECT changeHour(toDate('2000-01-01'), 24); -- out-of-bounds
SELECT '-- Date32';
SELECT changeHour(toDate32('2000-01-01'), 0);
SELECT changeHour(toDate32('2000-01-01'), 2);
SELECT changeHour(toDate32('2000-01-01'), 23);
SELECT changeHour(toDate32('2000-01-01'), -1); -- out-of-bounds
SELECT changeHour(toDate32('2000-01-01'), 24); -- out-of-bounds
SELECT '-- DateTime';
SELECT changeHour(toDateTime('2000-01-01 11:22:33'), 0);
SELECT changeHour(toDateTime('2000-01-01 11:22:33'), 2);
SELECT changeHour(toDateTime('2000-01-01 11:22:33'), 23);
SELECT changeHour(toDateTime('2000-01-01 11:22:33'), -1); -- out-of-bounds
SELECT changeHour(toDateTime('2000-01-01 11:22:33'), 24); -- out-of-bounds
SELECT '-- DateTime64';
SELECT changeHour(toDateTime64('2000-01-01 11:22:33.4444', 4), 0);
SELECT changeHour(toDateTime64('2000-01-01 11:22:33.4444', 4), 2);
SELECT changeHour(toDateTime64('2000-01-01 11:22:33.4444', 4), 23);
SELECT changeHour(toDateTime64('2000-01-01 11:22:33.4444', 4), -1); -- out-of-bounds
SELECT changeHour(toDateTime64('2000-01-01 11:22:33.4444', 4), 24); -- out-of-bounds
SELECT '-- With different timezone';
SELECT changeHour(toDate('2000-01-01'), -1) SETTINGS session_timezone = 'Asia/Novosibirsk';
SELECT changeHour(toDate('2000-01-01'), 24) SETTINGS session_timezone = 'Asia/Novosibirsk';

SELECT 'changeMinute';
SELECT '-- Date';
SELECT changeMinute(toDate('2000-01-01'), 0);
SELECT changeMinute(toDate('2000-01-01'), 2);
SELECT changeMinute(toDate('2000-01-01'), 59);
SELECT changeMinute(toDate('2000-01-01'), -1); -- out-of-bounds
SELECT changeMinute(toDate('2000-01-01'), 60); -- out-of-bounds
SELECT '-- Date32';
SELECT changeMinute(toDate32('2000-01-01'), 0);
SELECT changeMinute(toDate32('2000-01-01'), 2);
SELECT changeMinute(toDate32('2000-01-01'), 59);
SELECT changeMinute(toDate32('2000-01-01'), -1); -- out-of-bounds
SELECT changeMinute(toDate32('2000-01-01'), 60); -- out-of-bounds
SELECT '-- DateTime';
SELECT changeMinute(toDateTime('2000-01-01 11:22:33'), 0);
SELECT changeMinute(toDateTime('2000-01-01 11:22:33'), 2);
SELECT changeMinute(toDateTime('2000-01-01 11:22:33'), 59);
SELECT changeMinute(toDateTime('2000-01-01 11:22:33'), -1); -- out-of-bounds
SELECT changeMinute(toDateTime('2000-01-01 11:22:33'), 60); -- out-of-bounds
SELECT '-- DateTime64';
SELECT changeMinute(toDateTime64('2000-01-01 11:22:33.4444', 4), 0);
SELECT changeMinute(toDateTime64('2000-01-01 11:22:33.4444', 4), 2);
SELECT changeMinute(toDateTime64('2000-01-01 11:22:33.4444', 4), 59);
SELECT changeMinute(toDateTime64('2000-01-01 11:22:33.4444', 4), -1); -- out-of-bounds
SELECT changeMinute(toDateTime64('2000-01-01 11:22:33.4444', 4), 60); -- out-of-bounds
SELECT '-- With different timezone';
SELECT changeMinute(toDate('2000-01-01'), -1) SETTINGS session_timezone = 'Asia/Novosibirsk';
SELECT changeMinute(toDate('2000-01-01'), 60) SETTINGS session_timezone = 'Asia/Novosibirsk';

SELECT 'changeSecond';
SELECT '-- Date';
SELECT changeSecond(toDate('2000-01-01'), 0);
SELECT changeSecond(toDate('2000-01-01'), 2);
SELECT changeSecond(toDate('2000-01-01'), 59);
SELECT changeSecond(toDate('2000-01-01'), -1); -- out-of-bounds
SELECT changeSecond(toDate('2000-01-01'), 60); -- out-of-bounds
SELECT '-- Date32';
SELECT changeSecond(toDate32('2000-01-01'), 0);
SELECT changeSecond(toDate32('2000-01-01'), 2);
SELECT changeSecond(toDate32('2000-01-01'), 59);
SELECT changeSecond(toDate32('2000-01-01'), -1); -- out-of-bounds
SELECT changeSecond(toDate32('2000-01-01'), 60); -- out-of-bounds
SELECT '-- DateTime';
SELECT changeSecond(toDateTime('2000-01-01 11:22:33'), 0);
SELECT changeSecond(toDateTime('2000-01-01 11:22:33'), 2);
SELECT changeSecond(toDateTime('2000-01-01 11:22:33'), 59);
SELECT changeSecond(toDateTime('2000-01-01 11:22:33'), -1); -- out-of-bounds
SELECT changeSecond(toDateTime('2000-01-01 11:22:33'), 60); -- out-of-bounds
SELECT '-- DateTime64';
SELECT changeSecond(toDateTime64('2000-01-01 11:22:33.4444', 4), 0);
SELECT changeSecond(toDateTime64('2000-01-01 11:22:33.4444', 4), 2);
SELECT changeSecond(toDateTime64('2000-01-01 11:22:33.4444', 4), 59);
SELECT changeSecond(toDateTime64('2000-01-01 11:22:33.4444', 4), -1); -- out-of-bounds
SELECT changeSecond(toDateTime64('2000-01-01 11:22:33.4444', 4), 60); -- out-of-bounds
SELECT '-- With different timezone';
SELECT changeSecond(toDate('2000-01-01'), -1) SETTINGS session_timezone = 'Asia/Novosibirsk';
SELECT changeSecond(toDate('2000-01-01'), 60) SETTINGS session_timezone = 'Asia/Novosibirsk';
