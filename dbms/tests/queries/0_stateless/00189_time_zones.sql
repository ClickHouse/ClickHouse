
/* timestamp 1419800400 == 2014-12-29 00:00:00 (Europe/Moscow) */
/* timestamp 1412106600 == 2014-09-30 23:50:00 (Europe/Moscow) */
/* timestamp 1420102800 == 2015-01-01 12:00:00 (Europe/Moscow) */
/* timestamp 1428310800 == 2015-04-06 12:00:00 (Europe/Moscow) */
/* timestamp 1436956200 == 2015-07-15 13:30:00 (Europe/Moscow) */
/* timestamp 1426415400 == 2015-03-15 13:30:00 (Europe/Moscow) */

/* toMonday */

SELECT toMonday(toDateTime(1419800400), 'Europe/Moscow');
SELECT toMonday(toDateTime(1419800400), 'Europe/Paris');
SELECT toMonday(toDateTime(1419800400), 'Europe/London');
SELECT toMonday(toDateTime(1419800400), 'Asia/Tokyo');
SELECT toMonday(toDateTime(1419800400), 'Pacific/Pitcairn');

/* toStartOfMonth */

SELECT toStartOfMonth(toDateTime(1419800400), 'Europe/Moscow');
SELECT toStartOfMonth(toDateTime(1419800400), 'Europe/Paris');
SELECT toStartOfMonth(toDateTime(1419800400), 'Europe/London');
SELECT toStartOfMonth(toDateTime(1419800400), 'Asia/Tokyo');
SELECT toStartOfMonth(toDateTime(1419800400), 'Pacific/Pitcairn');

/* toStartOfQuarter */

SELECT toStartOfMonth(toDateTime(1419800400), 'Europe/Moscow');
SELECT toStartOfMonth(toDateTime(1419800400), 'Europe/Paris');
SELECT toStartOfMonth(toDateTime(1419800400), 'Europe/London');
SELECT toStartOfMonth(toDateTime(1419800400), 'Asia/Tokyo');
SELECT toStartOfMonth(toDateTime(1419800400), 'Pacific/Pitcairn');

/* toStartOfYear */

SELECT toStartOfQuarter(toDateTime(1412106600), 'Europe/Moscow');
SELECT toStartOfQuarter(toDateTime(1412106600), 'Europe/Paris');
SELECT toStartOfQuarter(toDateTime(1412106600), 'Europe/London');
SELECT toStartOfQuarter(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toStartOfQuarter(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toTime */

SELECT toTime(toDateTime(1420102800), 'Europe/Moscow'), toTime(toDateTime(1428310800), 'Europe/Moscow');
SELECT toTime(toDateTime(1420102800), 'Europe/Paris'), toTime(toDateTime(1428310800), 'Europe/Paris');
SELECT toTime(toDateTime(1420102800), 'Europe/London'), toTime(toDateTime(1428310800), 'Europe/London');
SELECT toTime(toDateTime(1420102800), 'Asia/Tokyo'), toTime(toDateTime(1428310800), 'Asia/Tokyo');
SELECT toTime(toDateTime(1420102800), 'Pacific/Pitcairn'), toTime(toDateTime(1428310800), 'Pacific/Pitcairn');

/* toYear */

SELECT toYear(toDateTime(1412106600), 'Europe/Moscow');
SELECT toYear(toDateTime(1412106600), 'Europe/Paris');
SELECT toYear(toDateTime(1412106600), 'Europe/London');
SELECT toYear(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toYear(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toMonth */

SELECT toMonth(toDateTime(1412106600), 'Europe/Moscow');
SELECT toMonth(toDateTime(1412106600), 'Europe/Paris');
SELECT toMonth(toDateTime(1412106600), 'Europe/London');
SELECT toMonth(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toMonth(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toDayOfMonth */

SELECT toDayOfMonth(toDateTime(1412106600), 'Europe/Moscow');
SELECT toDayOfMonth(toDateTime(1412106600), 'Europe/Paris');
SELECT toDayOfMonth(toDateTime(1412106600), 'Europe/London');
SELECT toDayOfMonth(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toDayOfMonth(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toDayOfWeek */

SELECT toDayOfWeek(toDateTime(1412106600), 'Europe/Moscow');
SELECT toDayOfWeek(toDateTime(1412106600), 'Europe/Paris');
SELECT toDayOfWeek(toDateTime(1412106600), 'Europe/London');
SELECT toDayOfWeek(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toDayOfWeek(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toHour */

SELECT toHour(toDateTime(1412106600), 'Europe/Moscow');
SELECT toHour(toDateTime(1412106600), 'Europe/Paris');
SELECT toHour(toDateTime(1412106600), 'Europe/London');
SELECT toHour(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toHour(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toMinute */

SELECT toMinute(toDateTime(1412106600), 'Europe/Moscow');
SELECT toMinute(toDateTime(1412106600), 'Europe/Paris');
SELECT toMinute(toDateTime(1412106600), 'Europe/London');
SELECT toMinute(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toMinute(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toSecond */

SELECT toSecond(toDateTime(1412106600), 'Europe/Moscow');
SELECT toSecond(toDateTime(1412106600), 'Europe/Paris');
SELECT toSecond(toDateTime(1412106600), 'Europe/London');
SELECT toSecond(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toSecond(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toStartOfMinute */

SELECT toStartOfMinute(toDateTime(1412106600), 'Europe/Moscow');
SELECT toStartOfMinute(toDateTime(1412106600), 'Europe/Paris');
SELECT toStartOfMinute(toDateTime(1412106600), 'Europe/London');
SELECT toStartOfMinute(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toStartOfMinute(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toStartOfHour */

SELECT toStartOfHour(toDateTime(1412106600), 'Europe/Moscow');
SELECT toStartOfHour(toDateTime(1412106600), 'Europe/Paris');
SELECT toStartOfHour(toDateTime(1412106600), 'Europe/London');
SELECT toStartOfHour(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toStartOfHour(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toRelativeYearNum */

SELECT toRelativeYearNum(toDateTime(1412106600), 'Europe/Moscow');
SELECT toRelativeYearNum(toDateTime(1412106600), 'Europe/Paris');
SELECT toRelativeYearNum(toDateTime(1412106600), 'Europe/London');
SELECT toRelativeYearNum(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toRelativeYearNum(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toRelativeMonthNum */

SELECT toRelativeMonthNum(toDateTime(1412106600), 'Europe/Moscow');
SELECT toRelativeMonthNum(toDateTime(1412106600), 'Europe/Paris');
SELECT toRelativeMonthNum(toDateTime(1412106600), 'Europe/London');
SELECT toRelativeMonthNum(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toRelativeMonthNum(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toRelativeWeekNum */

SELECT toRelativeWeekNum(toDateTime(1412106600), 'Europe/Moscow');
SELECT toRelativeWeekNum(toDateTime(1412106600), 'Europe/Paris');
SELECT toRelativeWeekNum(toDateTime(1412106600), 'Europe/London');
SELECT toRelativeWeekNum(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toRelativeWeekNum(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toRelativeDayNum */

SELECT toRelativeDayNum(toDateTime(1412106600), 'Europe/Moscow');
SELECT toRelativeDayNum(toDateTime(1412106600), 'Europe/Paris');
SELECT toRelativeDayNum(toDateTime(1412106600), 'Europe/London');
SELECT toRelativeDayNum(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toRelativeDayNum(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toRelativeHourNum */

SELECT toRelativeHourNum(toDateTime(1412106600), 'Europe/Moscow');
SELECT toRelativeHourNum(toDateTime(1412106600), 'Europe/Paris');
SELECT toRelativeHourNum(toDateTime(1412106600), 'Europe/London');
SELECT toRelativeHourNum(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toRelativeHourNum(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toRelativeMinuteNum */

SELECT toRelativeMinuteNum(toDateTime(1412106600), 'Europe/Moscow');
SELECT toRelativeMinuteNum(toDateTime(1412106600), 'Europe/Paris');
SELECT toRelativeMinuteNum(toDateTime(1412106600), 'Europe/London');
SELECT toRelativeMinuteNum(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toRelativeMinuteNum(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toRelativeSecondNum */

SELECT toRelativeSecondNum(toDateTime(1412106600), 'Europe/Moscow');
SELECT toRelativeSecondNum(toDateTime(1412106600), 'Europe/Paris');
SELECT toRelativeSecondNum(toDateTime(1412106600), 'Europe/London');
SELECT toRelativeSecondNum(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toRelativeSecondNum(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toDate */

SELECT toDate(toDateTime(1412106600), 'Europe/Moscow');
SELECT toDate(toDateTime(1412106600), 'Europe/Paris');
SELECT toDate(toDateTime(1412106600), 'Europe/London');
SELECT toDate(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toDate(toDateTime(1412106600), 'Pacific/Pitcairn');

SELECT toDate(1412106600, 'Europe/Moscow');
SELECT toDate(1412106600, 'Europe/Paris');
SELECT toDate(1412106600, 'Europe/London');
SELECT toDate(1412106600, 'Asia/Tokyo');
SELECT toDate(1412106600, 'Pacific/Pitcairn');

DROP TABLE IF EXISTS foo;
CREATE TABLE foo(x Int32, y String) ENGINE=Memory;
INSERT INTO foo(x, y) VALUES(1420102800, 'Europe/Moscow');
INSERT INTO foo(x, y) VALUES(1412106600, 'Europe/Paris');
INSERT INTO foo(x, y) VALUES(1419800400, 'Europe/London');
INSERT INTO foo(x, y) VALUES(1436956200, 'Asia/Tokyo');
INSERT INTO foo(x, y) VALUES(1426415400, 'Pacific/Pitcairn');

SELECT toMonday(toDateTime(x), y), toStartOfMonth(toDateTime(x), y), toStartOfQuarter(toDateTime(x), y), toTime(toDateTime(x), y) FROM foo ORDER BY y ASC;
SELECT toYear(toDateTime(x), y), toMonth(toDateTime(x), y), toDayOfMonth(toDateTime(x), y), toDayOfWeek(toDateTime(x), y) FROM foo ORDER BY y ASC;
SELECT toHour(toDateTime(x), y), toMinute(toDateTime(x), y), toSecond(toDateTime(x), y), toStartOfMinute(toDateTime(x), y) FROM foo ORDER BY y ASC;
SELECT toStartOfHour(toDateTime(x), y), toRelativeYearNum(toDateTime(x), y), toRelativeMonthNum(toDateTime(x), y), toRelativeWeekNum(toDateTime(x), y) FROM foo ORDER BY y ASC;
SELECT toRelativeDayNum(toDateTime(x), y), toRelativeHourNum(toDateTime(x), y), toRelativeMinuteNum(toDateTime(x), y), toRelativeSecondNum(toDateTime(x), y) FROM foo ORDER BY y ASC;
SELECT toDate(toDateTime(x), y), toDate(x, y) FROM foo ORDER BY y ASC;

SELECT toMonday(toDateTime(x), 'Europe/Paris'), toStartOfMonth(toDateTime(x), 'Europe/London'), toStartOfQuarter(toDateTime(x), 'Asia/Tokyo'), toTime(toDateTime(x), 'Pacific/Pitcairn') FROM foo ORDER BY x ASC;
SELECT toYear(toDateTime(x), 'Europe/Paris'), toMonth(toDateTime(x), 'Europe/London'), toDayOfMonth(toDateTime(x), 'Asia/Tokyo'), toDayOfWeek(toDateTime(x), 'Pacific/Pitcairn') FROM foo ORDER BY y ASC;
SELECT toHour(toDateTime(x), 'Europe/Paris'), toMinute(toDateTime(x), 'Europe/London'), toSecond(toDateTime(x), 'Asia/Tokyo'), toStartOfMinute(toDateTime(x), 'Pacific/Pitcairn') FROM foo ORDER BY y ASC;
SELECT toStartOfHour(toDateTime(x), 'Europe/Paris'), toRelativeYearNum(toDateTime(x), 'Europe/London'), toRelativeMonthNum(toDateTime(x), 'Asia/Tokyo'), toRelativeWeekNum(toDateTime(x), 'Pacific/Pitcairn') FROM foo ORDER BY y ASC;
SELECT toRelativeDayNum(toDateTime(x), 'Europe/Paris'), toRelativeHourNum(toDateTime(x), 'Europe/London'), toRelativeMinuteNum(toDateTime(x), 'Asia/Tokyo'), toRelativeSecondNum(toDateTime(x), 'Pacific/Pitcairn') FROM foo ORDER BY y ASC;
SELECT toDate(toDateTime(x), 'Europe/Paris'), toDate(x, 'Europe/Paris') FROM foo ORDER BY y ASC;

SELECT toMonday(toDateTime(1426415400), y), toStartOfMonth(toDateTime(1426415400), y), toStartOfQuarter(toDateTime(1426415400), y), toTime(toDateTime(1426415400), y) FROM foo ORDER BY y ASC;
SELECT toYear(toDateTime(1426415400), y), toMonth(toDateTime(1426415400), y), toDayOfMonth(toDateTime(1426415400), y), toDayOfWeek(toDateTime(1426415400), y) FROM foo ORDER BY y ASC;
SELECT toHour(toDateTime(1426415400), y), toMinute(toDateTime(1426415400), y), toSecond(toDateTime(1426415400), y), toStartOfMinute(toDateTime(1426415400), y) FROM foo ORDER BY y ASC;
SELECT toStartOfHour(toDateTime(1426415400), y), toRelativeYearNum(toDateTime(1426415400), y), toRelativeMonthNum(toDateTime(1426415400), y), toRelativeWeekNum(toDateTime(1426415400), y) FROM foo ORDER BY y ASC;
SELECT toRelativeDayNum(toDateTime(1426415400), y), toRelativeHourNum(toDateTime(1426415400), y), toRelativeMinuteNum(toDateTime(1426415400), y), toRelativeSecondNum(toDateTime(1426415400), y) FROM foo ORDER BY y ASC;
SELECT toDate(toDateTime(1426415400), y), toDate(1426415400, y) FROM foo ORDER BY y ASC;

/* toString */

SELECT toString(toDateTime(1436956200), 'Europe/Moscow');
SELECT toString(toDateTime(1436956200), 'Europe/Paris');
SELECT toString(toDateTime(1436956200), 'Europe/London');
SELECT toString(toDateTime(1436956200), 'Asia/Tokyo');
SELECT toString(toDateTime(1436956200), 'Pacific/Pitcairn');

SELECT toString(toDateTime(x), y) FROM foo ORDER BY y ASC;
SELECT toString(toDateTime(1436956200), y) FROM foo ORDER BY y ASC;
SELECT toString(toDateTime(x), 'Europe/London') FROM foo ORDER BY x ASC;

/* toUnixTimestamp */

SELECT toUnixTimestamp(toString(toDateTime(1426415400)), 'Europe/Moscow');
SELECT toUnixTimestamp(toString(toDateTime(1426415400)), 'Europe/Paris');
SELECT toUnixTimestamp(toString(toDateTime(1426415400)), 'Europe/London');
SELECT toUnixTimestamp(toString(toDateTime(1426415400)), 'Asia/Tokyo');
SELECT toUnixTimestamp(toString(toDateTime(1426415400)), 'Pacific/Pitcairn');

SELECT toUnixTimestamp(toString(toDateTime(1426415400), 'Europe/Moscow'), 'Europe/Moscow');
SELECT toUnixTimestamp(toString(toDateTime(1426415400), 'Europe/Paris'), 'Europe/Paris');
SELECT toUnixTimestamp(toString(toDateTime(1426415400), 'Europe/London'), 'Europe/London');
SELECT toUnixTimestamp(toString(toDateTime(1426415400), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT toUnixTimestamp(toString(toDateTime(1426415400), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

SELECT toUnixTimestamp(toString(toDateTime(x)), y) FROM foo ORDER BY y ASC;
SELECT toUnixTimestamp(toString(toDateTime(1426415400)), y) FROM foo ORDER BY y ASC;
SELECT toUnixTimestamp(toString(toDateTime(x)), 'Europe/Paris') FROM foo ORDER BY x ASC;
