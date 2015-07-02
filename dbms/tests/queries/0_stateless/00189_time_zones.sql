
/* toMonday */

SELECT toMonday(toDateTime('2014-12-29 00:00:00'), 'Europe/Moscow');
SELECT toMonday(toDateTime('2014-12-29 00:00:00'), 'Europe/Paris');
SELECT toMonday(toDateTime('2014-12-29 00:00:00'), 'Europe/London');
SELECT toMonday(toDateTime('2014-12-29 00:00:00'), 'Asia/Tokyo');
SELECT toMonday(toDateTime('2014-12-29 00:00:00'), 'Pacific/Pitcairn');

/* toStartOfMonth */

SELECT toStartOfMonth(toDateTime('2014-12-29 00:00:00'), 'Europe/Moscow');
SELECT toStartOfMonth(toDateTime('2014-12-29 00:00:00'), 'Europe/Paris');
SELECT toStartOfMonth(toDateTime('2014-12-29 00:00:00'), 'Europe/London');
SELECT toStartOfMonth(toDateTime('2014-12-29 00:00:00'), 'Asia/Tokyo');
SELECT toStartOfMonth(toDateTime('2014-12-29 00:00:00'), 'Pacific/Pitcairn');

/* toStartOfQuarter */

SELECT toStartOfMonth(toDateTime('2014-12-29 00:00:00'), 'Europe/Moscow');
SELECT toStartOfMonth(toDateTime('2014-12-29 00:00:00'), 'Europe/Paris');
SELECT toStartOfMonth(toDateTime('2014-12-29 00:00:00'), 'Europe/London');
SELECT toStartOfMonth(toDateTime('2014-12-29 00:00:00'), 'Asia/Tokyo');
SELECT toStartOfMonth(toDateTime('2014-12-29 00:00:00'), 'Pacific/Pitcairn');

/* toStartOfYear */

SELECT toStartOfQuarter(toDateTime('2014-09-30 23:50:00'), 'Europe/Moscow');
SELECT toStartOfQuarter(toDateTime('2014-09-30 23:50:00'), 'Europe/Paris');
SELECT toStartOfQuarter(toDateTime('2014-09-30 23:50:00'), 'Europe/London');
SELECT toStartOfQuarter(toDateTime('2014-09-30 23:50:00'), 'Asia/Tokyo');
SELECT toStartOfQuarter(toDateTime('2014-09-30 23:50:00'), 'Pacific/Pitcairn');

/* toTime */

SELECT toTime(toDateTime('2015-01-01 12:00:00'), 'Europe/Moscow'), toTime(toDateTime('2015-04-06 12:00:00'), 'Europe/Moscow');
SELECT toTime(toDateTime('2015-01-01 12:00:00'), 'Europe/Paris'), toTime(toDateTime('2015-04-06 12:00:00'), 'Europe/Paris');
SELECT toTime(toDateTime('2015-01-01 12:00:00'), 'Europe/London'), toTime(toDateTime('2015-04-06 12:00:00'), 'Europe/London');
SELECT toTime(toDateTime('2015-01-01 12:00:00'), 'Asia/Tokyo'), toTime(toDateTime('2015-04-06 12:00:00'), 'Asia/Tokyo');
SELECT toTime(toDateTime('2015-01-01 12:00:00'), 'Pacific/Pitcairn'), toTime(toDateTime('2015-04-06 12:00:00'), 'Pacific/Pitcairn');

/* toString */

SELECT toString(toDateTime('2015-07-15 13:30:00'), 'Europe/Moscow');
SELECT toString(toDateTime('2015-07-15 13:30:00'), 'Europe/Paris');
SELECT toString(toDateTime('2015-07-15 13:30:00'), 'Europe/London');
SELECT toString(toDateTime('2015-07-15 13:30:00'), 'Asia/Tokyo');
SELECT toString(toDateTime('2015-07-15 13:30:00'), 'Pacific/Pitcairn');

/* toUnixTimestamp */

SELECT toUnixTimestamp('2015-03-15 13:30:00', 'Europe/Moscow');
SELECT toUnixTimestamp('2015-03-15 13:30:00', 'Europe/Paris');
SELECT toUnixTimestamp('2015-03-15 13:30:00', 'Europe/London');
SELECT toUnixTimestamp('2015-03-15 13:30:00', 'Asia/Tokyo');
SELECT toUnixTimestamp('2015-03-15 13:30:00', 'Pacific/Pitcairn');

