
/* timestamp 1419800400 == 2014-12-29 00:00:00 (Europe/Moscow) */
/* timestamp 1412106600 == 2014-09-30 23:50:00 (Europe/Moscow) */
/* timestamp 1420102800 == 2015-01-01 12:00:00 (Europe/Moscow) */
/* timestamp 1428310800 == 2015-04-06 12:00:00 (Europe/Moscow) */
/* timestamp 1436956200 == 2015-07-15 13:30:00 (Europe/Moscow) */
/* timestamp 1426415400 == 2015-03-15 13:30:00 (Europe/Moscow) */
/* timestamp 1549483055 == 2019-02-06 22:57:35 (Europe/Moscow) */
/* date 16343 == 2014-09-30 */
/* date 16433 == 2014-12-29 */
/* date 17933 == 2019-02-06 */

/* toStartOfDay */

SELECT 'toStartOfDay';
SELECT toStartOfDay(toDateTime(1412106600), 'Europe/Moscow');
SELECT toStartOfDay(toDateTime(1412106600), 'Europe/Paris');
SELECT toStartOfDay(toDateTime(1412106600), 'Europe/London');
SELECT toStartOfDay(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toStartOfDay(toDateTime(1412106600), 'Pacific/Pitcairn');
SELECT toStartOfDay(toDate(16343), 'Europe/Moscow');
SELECT toStartOfDay(toDate(16343), 'Europe/Paris');
SELECT toStartOfDay(toDate(16343), 'Europe/London');
SELECT toStartOfDay(toDate(16343), 'Asia/Tokyo');
SELECT toStartOfDay(toDate(16343), 'Pacific/Pitcairn');

/* toMonday */

SELECT 'toMonday';
SELECT toMonday(toDateTime(1419800400), 'Europe/Moscow');
SELECT toMonday(toDateTime(1419800400), 'Europe/Paris');
SELECT toMonday(toDateTime(1419800400), 'Europe/London');
SELECT toMonday(toDateTime(1419800400), 'Asia/Tokyo');
SELECT toMonday(toDateTime(1419800400), 'Pacific/Pitcairn');
SELECT toMonday(toDate(16433));
SELECT toMonday(toDate(16433));
SELECT toMonday(toDate(16433));
SELECT toMonday(toDate(16433));
SELECT toMonday(toDate(16433));

/* toStartOfMonth */

SELECT 'toStartOfMonth';
SELECT toStartOfMonth(toDateTime(1419800400), 'Europe/Moscow');
SELECT toStartOfMonth(toDateTime(1419800400), 'Europe/Paris');
SELECT toStartOfMonth(toDateTime(1419800400), 'Europe/London');
SELECT toStartOfMonth(toDateTime(1419800400), 'Asia/Tokyo');
SELECT toStartOfMonth(toDateTime(1419800400), 'Pacific/Pitcairn');
SELECT toStartOfMonth(toDate(16433));
SELECT toStartOfMonth(toDate(16433));
SELECT toStartOfMonth(toDate(16433));
SELECT toStartOfMonth(toDate(16433));
SELECT toStartOfMonth(toDate(16433));

/* toStartOfQuarter */

SELECT 'toStartOfQuarter';
SELECT toStartOfQuarter(toDateTime(1412106600), 'Europe/Moscow');
SELECT toStartOfQuarter(toDateTime(1412106600), 'Europe/Paris');
SELECT toStartOfQuarter(toDateTime(1412106600), 'Europe/London');
SELECT toStartOfQuarter(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toStartOfQuarter(toDateTime(1412106600), 'Pacific/Pitcairn');
SELECT toStartOfQuarter(toDate(16343));
SELECT toStartOfQuarter(toDate(16343));
SELECT toStartOfQuarter(toDate(16343));
SELECT toStartOfQuarter(toDate(16343));
SELECT toStartOfQuarter(toDate(16343));

/* toStartOfYear */

SELECT 'toStartOfYear';
SELECT toStartOfYear(toDateTime(1419800400), 'Europe/Moscow');
SELECT toStartOfYear(toDateTime(1419800400), 'Europe/Paris');
SELECT toStartOfYear(toDateTime(1419800400), 'Europe/London');
SELECT toStartOfYear(toDateTime(1419800400), 'Asia/Tokyo');
SELECT toStartOfYear(toDateTime(1419800400), 'Pacific/Pitcairn');
SELECT toStartOfYear(toDate(16433));
SELECT toStartOfYear(toDate(16433));
SELECT toStartOfYear(toDate(16433));
SELECT toStartOfYear(toDate(16433));
SELECT toStartOfYear(toDate(16433));

/* toTime */

SELECT 'toTime';
SELECT toString(toTime(toDateTime(1420102800), 'Europe/Moscow'), 'Europe/Moscow'), toString(toTime(toDateTime(1428310800), 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toTime(toDateTime(1420102800), 'Europe/Paris'), 'Europe/Paris'), toString(toTime(toDateTime(1428310800), 'Europe/Paris'), 'Europe/Paris');
SELECT toString(toTime(toDateTime(1420102800), 'Europe/London'), 'Europe/London'), toString(toTime(toDateTime(1428310800), 'Europe/London'), 'Europe/London');
SELECT toString(toTime(toDateTime(1420102800), 'Asia/Tokyo'), 'Asia/Tokyo'), toString(toTime(toDateTime(1428310800), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT toString(toTime(toDateTime(1420102800), 'Pacific/Pitcairn'), 'Pacific/Pitcairn'), toString(toTime(toDateTime(1428310800), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

/* toYear */

SELECT 'toYear';
SELECT toYear(toDateTime(1412106600), 'Europe/Moscow');
SELECT toYear(toDateTime(1412106600), 'Europe/Paris');
SELECT toYear(toDateTime(1412106600), 'Europe/London');
SELECT toYear(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toYear(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toMonth */

SELECT 'toMonth';
SELECT toMonth(toDateTime(1412106600), 'Europe/Moscow');
SELECT toMonth(toDateTime(1412106600), 'Europe/Paris');
SELECT toMonth(toDateTime(1412106600), 'Europe/London');
SELECT toMonth(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toMonth(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toDayOfMonth */

SELECT 'toDayOfMonth';
SELECT toDayOfMonth(toDateTime(1412106600), 'Europe/Moscow');
SELECT toDayOfMonth(toDateTime(1412106600), 'Europe/Paris');
SELECT toDayOfMonth(toDateTime(1412106600), 'Europe/London');
SELECT toDayOfMonth(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toDayOfMonth(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toDayOfWeek */

SELECT 'toDayOfWeek';
SELECT toDayOfWeek(toDateTime(1412106600), 'Europe/Moscow');
SELECT toDayOfWeek(toDateTime(1412106600), 'Europe/Paris');
SELECT toDayOfWeek(toDateTime(1412106600), 'Europe/London');
SELECT toDayOfWeek(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toDayOfWeek(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toHour */

SELECT 'toHour';
SELECT toHour(toDateTime(1412106600), 'Europe/Moscow');
SELECT toHour(toDateTime(1412106600), 'Europe/Paris');
SELECT toHour(toDateTime(1412106600), 'Europe/London');
SELECT toHour(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toHour(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toMinute */

SELECT 'toMinute';
SELECT toMinute(toDateTime(1412106600), 'Europe/Moscow');
SELECT toMinute(toDateTime(1412106600), 'Europe/Paris');
SELECT toMinute(toDateTime(1412106600), 'Europe/London');
SELECT toMinute(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toMinute(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toSecond */

SELECT 'toSecond';
SELECT toSecond(toDateTime(1412106600), 'Europe/Moscow');
SELECT toSecond(toDateTime(1412106600), 'Europe/Paris');
SELECT toSecond(toDateTime(1412106600), 'Europe/London');
SELECT toSecond(toDateTime(1412106600), 'Asia/Tokyo');
SELECT toSecond(toDateTime(1412106600), 'Pacific/Pitcairn');

/* toStartOfMinute */

SELECT 'toStartOfMinute';
SELECT toString(toStartOfMinute(toDateTime(1549483055), 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfMinute(toDateTime(1549483055), 'Europe/Paris'), 'Europe/Paris');
SELECT toString(toStartOfMinute(toDateTime(1549483055), 'Europe/London'), 'Europe/London');
SELECT toString(toStartOfMinute(toDateTime(1549483055), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT toString(toStartOfMinute(toDateTime(1549483055), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

/* toStartOfFiveMinute */

SELECT 'toStartOfFiveMinute';
SELECT toString(toStartOfFiveMinute(toDateTime(1549483055), 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfFiveMinute(toDateTime(1549483055), 'Europe/Paris'), 'Europe/Paris');
SELECT toString(toStartOfFiveMinute(toDateTime(1549483055), 'Europe/London'), 'Europe/London');
SELECT toString(toStartOfFiveMinute(toDateTime(1549483055), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT toString(toStartOfFiveMinute(toDateTime(1549483055), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

/* toStartOfTenMinutes */

SELECT 'toStartOfTenMinutes';
SELECT toString(toStartOfTenMinutes(toDateTime(1549483055), 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfTenMinutes(toDateTime(1549483055), 'Europe/Paris'), 'Europe/Paris');
SELECT toString(toStartOfTenMinutes(toDateTime(1549483055), 'Europe/London'), 'Europe/London');
SELECT toString(toStartOfTenMinutes(toDateTime(1549483055), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT toString(toStartOfTenMinutes(toDateTime(1549483055), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

/* toStartOfFifteenMinutes */

SELECT 'toStartOfFifteenMinutes';
SELECT toString(toStartOfFifteenMinutes(toDateTime(1549483055), 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfFifteenMinutes(toDateTime(1549483055), 'Europe/Paris'), 'Europe/Paris');
SELECT toString(toStartOfFifteenMinutes(toDateTime(1549483055), 'Europe/London'), 'Europe/London');
SELECT toString(toStartOfFifteenMinutes(toDateTime(1549483055), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT toString(toStartOfFifteenMinutes(toDateTime(1549483055), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

/* toStartOfHour */

SELECT 'toStartOfHour';
SELECT toString(toStartOfHour(toDateTime(1549483055), 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfHour(toDateTime(1549483055), 'Europe/Paris'), 'Europe/Paris');
SELECT toString(toStartOfHour(toDateTime(1549483055), 'Europe/London'), 'Europe/London');
SELECT toString(toStartOfHour(toDateTime(1549483055), 'Asia/Tokyo'), 'Asia/Tokyo');
SELECT toString(toStartOfHour(toDateTime(1549483055), 'Pacific/Pitcairn'), 'Pacific/Pitcairn');

/* toStartOfInterval */

SELECT 'toStartOfInterval';
SELECT toStartOfInterval(toDateTime(1549483055), INTERVAL 1 year, 'Europe/Moscow');
SELECT toStartOfInterval(toDateTime(1549483055), INTERVAL 2 year, 'Europe/Moscow');
SELECT toStartOfInterval(toDateTime(1549483055), INTERVAL 5 year, 'Europe/Moscow');
SELECT toStartOfInterval(toDateTime(1549483055), INTERVAL 1 quarter, 'Europe/Moscow');
SELECT toStartOfInterval(toDateTime(1549483055), INTERVAL 2 quarter, 'Europe/Moscow');
SELECT toStartOfInterval(toDateTime(1549483055), INTERVAL 3 quarter, 'Europe/Moscow');
SELECT toStartOfInterval(toDateTime(1549483055), INTERVAL 1 month, 'Europe/Moscow');
SELECT toStartOfInterval(toDateTime(1549483055), INTERVAL 2 month, 'Europe/Moscow');
SELECT toStartOfInterval(toDateTime(1549483055), INTERVAL 5 month, 'Europe/Moscow');
SELECT toStartOfInterval(toDateTime(1549483055), INTERVAL 1 week, 'Europe/Moscow');
SELECT toStartOfInterval(toDateTime(1549483055), INTERVAL 2 week, 'Europe/Moscow');
SELECT toStartOfInterval(toDateTime(1549483055), INTERVAL 6 week, 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDateTime(1549483055), INTERVAL 1 day, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDateTime(1549483055), INTERVAL 2 day, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDateTime(1549483055), INTERVAL 5 day, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDateTime(1549483055), INTERVAL 1 hour, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDateTime(1549483055), INTERVAL 2 hour, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDateTime(1549483055), INTERVAL 6 hour, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDateTime(1549483055), INTERVAL 24 hour, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDateTime(1549483055), INTERVAL 1 minute, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDateTime(1549483055), INTERVAL 2 minute, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDateTime(1549483055), INTERVAL 5 minute, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDateTime(1549483055), INTERVAL 20 minute, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDateTime(1549483055), INTERVAL 90 minute, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDateTime(1549483055), INTERVAL 1 second, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDateTime(1549483055), INTERVAL 2 second, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDateTime(1549483055), INTERVAL 5 second, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toStartOfInterval(toDate(17933), INTERVAL 1 year);
SELECT toStartOfInterval(toDate(17933), INTERVAL 2 year);
SELECT toStartOfInterval(toDate(17933), INTERVAL 5 year);
SELECT toStartOfInterval(toDate(17933), INTERVAL 1 quarter);
SELECT toStartOfInterval(toDate(17933), INTERVAL 2 quarter);
SELECT toStartOfInterval(toDate(17933), INTERVAL 3 quarter);
SELECT toStartOfInterval(toDate(17933), INTERVAL 1 month);
SELECT toStartOfInterval(toDate(17933), INTERVAL 2 month);
SELECT toStartOfInterval(toDate(17933), INTERVAL 5 month);
SELECT toStartOfInterval(toDate(17933), INTERVAL 1 week);
SELECT toStartOfInterval(toDate(17933), INTERVAL 2 week);
SELECT toStartOfInterval(toDate(17933), INTERVAL 6 week);
SELECT toString(toStartOfInterval(toDate(17933), INTERVAL 1 day, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDate(17933), INTERVAL 2 day, 'Europe/Moscow'), 'Europe/Moscow');
SELECT toString(toStartOfInterval(toDate(17933), INTERVAL 5 day, 'Europe/Moscow'), 'Europe/Moscow');

/* toRelativeYearNum */

SELECT 'toRelativeYearNum';
SELECT toRelativeYearNum(toDateTime(1412106600), 'Europe/Moscow') - toRelativeYearNum(toDateTime(0), 'Europe/Moscow');
SELECT toRelativeYearNum(toDateTime(1412106600), 'Europe/Paris') - toRelativeYearNum(toDateTime(0), 'Europe/Paris');
SELECT toRelativeYearNum(toDateTime(1412106600), 'Europe/London') - toRelativeYearNum(toDateTime(0), 'Europe/London');
SELECT toRelativeYearNum(toDateTime(1412106600), 'Asia/Tokyo') - toRelativeYearNum(toDateTime(0), 'Asia/Tokyo');
SELECT toRelativeYearNum(toDateTime(1412106600), 'Pacific/Pitcairn') - toRelativeYearNum(toDateTime(0), 'Pacific/Pitcairn');

/* toRelativeMonthNum */

SELECT 'toRelativeMonthNum';
SELECT toRelativeMonthNum(toDateTime(1412106600), 'Europe/Moscow') - toRelativeMonthNum(toDateTime(0), 'Europe/Moscow');
SELECT toRelativeMonthNum(toDateTime(1412106600), 'Europe/Paris') - toRelativeMonthNum(toDateTime(0), 'Europe/Paris');
SELECT toRelativeMonthNum(toDateTime(1412106600), 'Europe/London') - toRelativeMonthNum(toDateTime(0), 'Europe/London');
SELECT toRelativeMonthNum(toDateTime(1412106600), 'Asia/Tokyo') - toRelativeMonthNum(toDateTime(0), 'Asia/Tokyo');
SELECT toRelativeMonthNum(toDateTime(1412106600), 'Pacific/Pitcairn') - toRelativeMonthNum(toDateTime(0), 'Pacific/Pitcairn');

/* toRelativeWeekNum */

SELECT 'toRelativeWeekNum';
SELECT toRelativeWeekNum(toDateTime(1412106600), 'Europe/Moscow') - toRelativeWeekNum(toDateTime(0), 'Europe/Moscow');
SELECT toRelativeWeekNum(toDateTime(1412106600), 'Europe/Paris') - toRelativeWeekNum(toDateTime(0), 'Europe/Paris');
SELECT toRelativeWeekNum(toDateTime(1412106600), 'Europe/London') - toRelativeWeekNum(toDateTime(0), 'Europe/London');
SELECT toRelativeWeekNum(toDateTime(1412106600), 'Asia/Tokyo') - toRelativeWeekNum(toDateTime(0), 'Asia/Tokyo');
SELECT toRelativeWeekNum(toDateTime(1412106600), 'Pacific/Pitcairn') - toRelativeWeekNum(toDateTime(0), 'Pacific/Pitcairn');

/* toRelativeDayNum */

SELECT 'toRelativeDayNum';
SELECT toRelativeDayNum(toDateTime(1412106600), 'Europe/Moscow') - toRelativeDayNum(toDateTime(0), 'Europe/Moscow');
SELECT toRelativeDayNum(toDateTime(1412106600), 'Europe/Paris') - toRelativeDayNum(toDateTime(0), 'Europe/Paris');
SELECT toRelativeDayNum(toDateTime(1412106600), 'Europe/London') - toRelativeDayNum(toDateTime(0), 'Europe/London');
SELECT toRelativeDayNum(toDateTime(1412106600), 'Asia/Tokyo') - toRelativeDayNum(toDateTime(0), 'Asia/Tokyo');
SELECT toRelativeDayNum(toDateTime(1412106600), 'Pacific/Pitcairn') - toRelativeDayNum(toDateTime(0), 'Pacific/Pitcairn');

/* toRelativeHourNum */

SELECT 'toRelativeHourNum';
SELECT toRelativeHourNum(toDateTime(1412106600), 'Europe/Moscow') - toRelativeHourNum(toDateTime(0), 'Europe/Moscow');
SELECT toRelativeHourNum(toDateTime(1412106600), 'Europe/Paris') - toRelativeHourNum(toDateTime(0), 'Europe/Paris');
SELECT toRelativeHourNum(toDateTime(1412106600), 'Europe/London') - toRelativeHourNum(toDateTime(0), 'Europe/London');
SELECT toRelativeHourNum(toDateTime(1412106600), 'Asia/Tokyo') - toRelativeHourNum(toDateTime(0), 'Asia/Tokyo');
-- known wrong result: SELECT toRelativeHourNum(toDateTime(1412106600), 'Pacific/Pitcairn') - toRelativeHourNum(toDateTime(0), 'Pacific/Pitcairn');

/* toRelativeMinuteNum */

SELECT 'toRelativeMinuteNum';
SELECT toRelativeMinuteNum(toDateTime(1412106600), 'Europe/Moscow') - toRelativeMinuteNum(toDateTime(0), 'Europe/Moscow');
SELECT toRelativeMinuteNum(toDateTime(1412106600), 'Europe/Paris') - toRelativeMinuteNum(toDateTime(0), 'Europe/Paris');
SELECT toRelativeMinuteNum(toDateTime(1412106600), 'Europe/London') - toRelativeMinuteNum(toDateTime(0), 'Europe/London');
SELECT toRelativeMinuteNum(toDateTime(1412106600), 'Asia/Tokyo') - toRelativeMinuteNum(toDateTime(0), 'Asia/Tokyo');
SELECT toRelativeMinuteNum(toDateTime(1412106600), 'Pacific/Pitcairn') - toRelativeMinuteNum(toDateTime(0), 'Pacific/Pitcairn');

/* toRelativeSecondNum */

SELECT 'toRelativeSecondNum';
SELECT toRelativeSecondNum(toDateTime(1412106600), 'Europe/Moscow') - toRelativeSecondNum(toDateTime(0), 'Europe/Moscow');
SELECT toRelativeSecondNum(toDateTime(1412106600), 'Europe/Paris') - toRelativeSecondNum(toDateTime(0), 'Europe/Paris');
SELECT toRelativeSecondNum(toDateTime(1412106600), 'Europe/London') - toRelativeSecondNum(toDateTime(0), 'Europe/London');
SELECT toRelativeSecondNum(toDateTime(1412106600), 'Asia/Tokyo') - toRelativeSecondNum(toDateTime(0), 'Asia/Tokyo');
SELECT toRelativeSecondNum(toDateTime(1412106600), 'Pacific/Pitcairn') - toRelativeSecondNum(toDateTime(0), 'Pacific/Pitcairn');

/* toDate */

SELECT 'toDate';
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

SELECT 'toString';
SELECT toString(toDateTime(1436956200), 'Europe/Moscow');
SELECT toString(toDateTime(1436956200), 'Europe/Paris');
SELECT toString(toDateTime(1436956200), 'Europe/London');
SELECT toString(toDateTime(1436956200), 'Asia/Tokyo');
SELECT toString(toDateTime(1436956200), 'Pacific/Pitcairn');

/* toUnixTimestamp */

SELECT 'toUnixTimestamp';
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
