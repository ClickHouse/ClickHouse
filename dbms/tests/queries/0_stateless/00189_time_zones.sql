
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

SELECT toString(toTime(toDateTime(1420102800), 'Europe/Moscow'), 'Europe/Moscow'), toString(toTime(toDateTime(1428310800), 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toTime(toDateTime(1420102800), 'Europe/Paris'), 'Europe/Paris'), toString(toTime(toDateTime(1428310800), 'Europe/Paris'), 'Europe/Paris');
SELECT toString(toTime(toDateTime(1420102800), 'Europe/London'), 'Europe/London'), toString(toTime(toDateTime(1428310800), 'Europe/London'), 'Europe/London');
SELECT toString(toTime(toDateTime(1420102800), 'Asia/Tokyo'), 'Asia/Tokyo'), toString(toTime(toDateTime(1428310800), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT toString(toTime(toDateTime(1420102800), 'Pacific/Pitcairn'), 'Pacific/Pitcairn'), toString(toTime(toDateTime(1428310800), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

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

SELECT toString(toStartOfMinute(toDateTime(1412106600), 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfMinute(toDateTime(1412106600), 'Europe/Paris'), 'Europe/Paris');
SELECT toString(toStartOfMinute(toDateTime(1412106600), 'Europe/London'), 'Europe/London');
SELECT toString(toStartOfMinute(toDateTime(1412106600), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT toString(toStartOfMinute(toDateTime(1412106600), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

/* toStartOfHour */

SELECT toString(toStartOfHour(toDateTime(1412106600), 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfHour(toDateTime(1412106600), 'Europe/Paris'), 'Europe/Paris');
SELECT toString(toStartOfHour(toDateTime(1412106600), 'Europe/London'), 'Europe/London');
SELECT toString(toStartOfHour(toDateTime(1412106600), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT toString(toStartOfHour(toDateTime(1412106600), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

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

/* toString */

SELECT toString(toDateTime(1436956200), 'Europe/Moscow');
SELECT toString(toDateTime(1436956200), 'Europe/Paris');
SELECT toString(toDateTime(1436956200), 'Europe/London');
SELECT toString(toDateTime(1436956200), 'Asia/Tokyo');
SELECT toString(toDateTime(1436956200), 'Pacific/Pitcairn');

/* toUnixTimestamp */

SELECT toUnixTimestamp(toString(toDateTime(1426415400), 'Europe/Moscow'), 'Europe/Moscow');
SELECT toUnixTimestamp(toString(toDateTime(1426415400), 'Europe/Moscow'), 'Europe/Paris');
SELECT toUnixTimestamp(toString(toDateTime(1426415400), 'Europe/Moscow'), 'Europe/London');
SELECT toUnixTimestamp(toString(toDateTime(1426415400), 'Europe/Moscow'), 'Asia/Tokyo');
SELECT toUnixTimestamp(toString(toDateTime(1426415400), 'Europe/Moscow'), 'Pacific/Pitcairn');

SELECT toUnixTimestamp(toString(toDateTime(1426415400), 'Europe/Moscow'), 'Europe/Moscow');
SELECT toUnixTimestamp(toString(toDateTime(1426415400), 'Europe/Paris'), 'Europe/Paris');
SELECT toUnixTimestamp(toString(toDateTime(1426415400), 'Europe/London'), 'Europe/London');
SELECT toUnixTimestamp(toString(toDateTime(1426415400), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT toUnixTimestamp(toString(toDateTime(1426415400), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');
