
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

DROP TABLE IF EXISTS foo;
CREATE TABLE foo(x Int32, y String) ENGINE=Memory;
INSERT INTO foo(x, y) VALUES(1420102800, 'Europe/Moscow');
INSERT INTO foo(x, y) VALUES(1412106600, 'Europe/Paris');
INSERT INTO foo(x, y) VALUES(1419800400, 'Europe/London');
INSERT INTO foo(x, y) VALUES(1436956200, 'Asia/Tokyo');
INSERT INTO foo(x, y) VALUES(1426415400, 'Pacific/Pitcairn');

SELECT toMonday(toDateTime(x), y), toStartOfMonth(toDateTime(x), y), toStartOfQuarter(toDateTime(x), y), toTime(toDateTime(x), y) FROM foo ORDER BY y ASC;
SELECT toMonday(toDateTime(x), 'Europe/Paris'), toStartOfMonth(toDateTime(x), 'Europe/London'), toStartOfQuarter(toDateTime(x), 'Asia/Tokyo'), toTime(toDateTime(x), 'Pacific/Pitcairn') FROM foo ORDER BY x ASC;
SELECT toMonday(toDateTime(1426415400), y), toStartOfMonth(toDateTime(1426415400), y), toStartOfQuarter(toDateTime(1426415400), y), toTime(toDateTime(1426415400), y) FROM foo ORDER BY y ASC;

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
