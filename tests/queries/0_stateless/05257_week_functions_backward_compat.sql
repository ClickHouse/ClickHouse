-- Backwards-compatibility regression test for week functions: records their current results for a handful of
-- date ranges, across input types and timezones, with and without an explicit mode. The reference was
-- recorded before the `week_functions_*` settings were added, and must not change.
-- Generated: the tests are one query per set of dates and function family, plus a few argument variants.

-- Calls that can't take a timezone argument (`String` inputs without a mode, KQL `datetime`) use from the session.
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_date;
CREATE TABLE t_date (d Date) ENGINE = Memory;
-- 10 days from Dec 27 around three New Years: Jan 1 2017 is a Sunday, Jan 1 2019 a Tuesday (together they tell
-- every first week rule and range apart), Jan 1 2027 a Friday (2026 has 53 ISO weeks). Then the first 7 days of
-- `Date` (1970-01-01, a Thursday) and the last 7 (up to 2149-06-06), where week starts and ends saturate.
INSERT INTO t_date SELECT toDate(day) FROM (SELECT toDate32('2016-12-27') + number AS day FROM numbers(10) UNION ALL SELECT toDate32('2018-12-27') + number AS day FROM numbers(10) UNION ALL SELECT toDate32('2026-12-27') + number AS day FROM numbers(10) UNION ALL SELECT toDate32('1970-01-01') + number AS day FROM numbers(7) UNION ALL SELECT toDate32('2149-05-31') + number AS day FROM numbers(7));
DROP TABLE IF EXISTS t_date32;
CREATE TABLE t_date32 (d Date32) ENGINE = Memory;
-- 10 days around the epoch (1969-12-29 is a Monday), the first and last 7 days of the lookup table (1900-2299),
-- and the first and last 7 days of the `Date32` range (0000-01-01, 9999-12-31), computed outside the table.
INSERT INTO t_date32 SELECT day FROM (SELECT toDate32('1969-12-29') + number AS day FROM numbers(10) UNION ALL SELECT toDate32('1900-01-01') + number AS day FROM numbers(7) UNION ALL SELECT toDate32('2299-12-25') + number AS day FROM numbers(7) UNION ALL SELECT toDate32('0000-01-01') + number AS day FROM numbers(7) UNION ALL SELECT toDate32('9999-12-25') + number AS day FROM numbers(7));
DROP TABLE IF EXISTS t_dt_utc;
CREATE TABLE t_dt_utc (d DateTime('UTC')) ENGINE = Memory;
-- The first 7 days of `DateTime` and 7 days before its end, then its last second: 4294967295 = 2^32 - 1,
-- 2106-02-07 06:28:15 UTC.
INSERT INTO t_dt_utc SELECT toDateTime(day, 'UTC') FROM (SELECT toDate32('1970-01-01') + number AS day FROM numbers(7) UNION ALL SELECT toDate32('2106-02-01') + number AS day FROM numbers(7));
INSERT INTO t_dt_utc VALUES (toDateTime(4294967295, 'UTC'));
DROP TABLE IF EXISTS t_dt64_utc;
CREATE TABLE t_dt64_utc (d DateTime64(3, 'UTC')) ENGINE = Memory;
-- As `t_date32`, then 23:59:59.999 (+ 86399999 ms) on the days around the epoch and on 0000-01-01 (the floor of a
-- negative fractional second), and the last millisecond of the range: 253402300799999 ms = 9999-12-31 23:59:59.999.
INSERT INTO t_dt64_utc SELECT toDateTime64(day, 3, 'UTC') FROM (SELECT toDate32('1969-12-29') + number AS day FROM numbers(10) UNION ALL SELECT toDate32('1900-01-01') + number AS day FROM numbers(7) UNION ALL SELECT toDate32('2299-12-25') + number AS day FROM numbers(7) UNION ALL SELECT toDate32('0000-01-01') + number AS day FROM numbers(7) UNION ALL SELECT toDate32('9999-12-25') + number AS day FROM numbers(7));
INSERT INTO t_dt64_utc SELECT toDateTime64(day, 3, 'UTC') + INTERVAL 86399999 MILLISECOND FROM (SELECT toDate32('1969-12-29') + number AS day FROM numbers(10) UNION ALL SELECT toDate32('0000-01-01') + number AS day FROM numbers(7));
INSERT INTO t_dt64_utc VALUES (fromUnixTimestamp64Milli(253402300799999, 'UTC'));
DROP TABLE IF EXISTS t_dt_kathmandu;
CREATE TABLE t_dt_kathmandu (d DateTime('Asia/Kathmandu')) ENGINE = Memory;
-- Kathmandu is UTC+5:45: local midnight and 23:59:59 (+ 86399 s) of 10 days around New Year 2027, so the local
-- day and the UTC day differ at one end of each day.
INSERT INTO t_dt_kathmandu SELECT toDateTime(day, 'Asia/Kathmandu') FROM (SELECT toDate32('2026-12-27') + number AS day FROM numbers(10));
INSERT INTO t_dt_kathmandu SELECT toDateTime(day, 'Asia/Kathmandu') + 86399 FROM (SELECT toDate32('2026-12-27') + number AS day FROM numbers(10));
DROP TABLE IF EXISTS t_string;
CREATE TABLE t_string (d String) ENGINE = Memory;
-- Sunday 2025-12-28, Thursday 2026-01-01, the last second of Saturday 2026-01-03 and the first of Sunday
-- 2026-01-04 (a week boundary for Sunday-first modes), Friday 2027-01-01.
INSERT INTO t_string VALUES ('2025-12-28'), ('2026-01-01'), ('2026-01-03 23:59:59'), ('2026-01-04 00:00:00'), ('2027-01-01');

SELECT 'Date: toWeek';
SELECT d,
    toWeek(d),
    toWeek(d, 0),
    toWeek(d, 1),
    toWeek(d, 2),
    toWeek(d, 3),
    toWeek(d, 4),
    toWeek(d, 5),
    toWeek(d, 6),
    toWeek(d, 7),
    toWeek(d, 8),
    toWeek(d, 9)
FROM t_date ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date: toYearWeek';
SELECT d,
    toYearWeek(d),
    toYearWeek(d, 0),
    toYearWeek(d, 1),
    toYearWeek(d, 2),
    toYearWeek(d, 3),
    toYearWeek(d, 4),
    toYearWeek(d, 5),
    toYearWeek(d, 6),
    toYearWeek(d, 7),
    toYearWeek(d, 8),
    toYearWeek(d, 9)
FROM t_date ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date: toStartOfWeek';
SELECT d,
    toStartOfWeek(d),
    toStartOfWeek(d, 0),
    toStartOfWeek(d, 1),
    toStartOfWeek(d, 2),
    toStartOfWeek(d, 3),
    toStartOfWeek(d, 4),
    toStartOfWeek(d, 5),
    toStartOfWeek(d, 6),
    toStartOfWeek(d, 7),
    toStartOfWeek(d, 8),
    toStartOfWeek(d, 9)
FROM t_date ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date: toLastDayOfWeek';
SELECT d,
    toLastDayOfWeek(d),
    toLastDayOfWeek(d, 0),
    toLastDayOfWeek(d, 1),
    toLastDayOfWeek(d, 2),
    toLastDayOfWeek(d, 3),
    toLastDayOfWeek(d, 4),
    toLastDayOfWeek(d, 5),
    toLastDayOfWeek(d, 6),
    toLastDayOfWeek(d, 7),
    toLastDayOfWeek(d, 8),
    toLastDayOfWeek(d, 9)
FROM t_date ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date: toDayOfWeek';
SELECT d,
    toDayOfWeek(d),
    toDayOfWeek(d, 0),
    toDayOfWeek(d, 1),
    toDayOfWeek(d, 2),
    toDayOfWeek(d, 3)
FROM t_date ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date: toRelativeWeekNum';
SELECT d,
    toRelativeWeekNum(d)
FROM t_date ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date: toStartOfInterval and date_trunc';
SELECT d,
    toStartOfInterval(d, INTERVAL 1 WEEK),
    toStartOfInterval(d, INTERVAL 2 WEEK),
    toStartOfInterval(d, INTERVAL 3 WEEK),
    toStartOfInterval(d, INTERVAL 1 WEEK, toDate('1970-01-01')),
    toStartOfInterval(d, INTERVAL 3 WEEK, toDate('1970-01-01')),
    date_trunc('week', d)
FROM t_date ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date: dateDiff and age';
SELECT d,
    dateDiff('week', toDate('2000-01-05'), d),
    dateDiff('week', d, toDate('2000-01-05')),
    age('week', toDate('2000-01-05'), d),
    age('week', d, toDate('2000-01-05'))
FROM t_date ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date: dateName and formatting';
SELECT d,
    dateName('week', d),
    dateName('weekday', d),
    formatDateTime(d, '%w %u %a'),
    formatDateTimeInJodaSyntax(d, 'e E')
FROM t_date ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date32: toWeek';
SELECT d,
    toWeek(d),
    toWeek(d, 0),
    toWeek(d, 1),
    toWeek(d, 2),
    toWeek(d, 3),
    toWeek(d, 4),
    toWeek(d, 5),
    toWeek(d, 6),
    toWeek(d, 7),
    toWeek(d, 8),
    toWeek(d, 9)
FROM t_date32 ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date32: toYearWeek';
SELECT d,
    toYearWeek(d),
    toYearWeek(d, 0),
    toYearWeek(d, 1),
    toYearWeek(d, 2),
    toYearWeek(d, 3),
    toYearWeek(d, 4),
    toYearWeek(d, 5),
    toYearWeek(d, 6),
    toYearWeek(d, 7),
    toYearWeek(d, 8),
    toYearWeek(d, 9)
FROM t_date32 ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date32: toStartOfWeek';
SELECT d,
    toStartOfWeek(d),
    toStartOfWeek(d, 0),
    toStartOfWeek(d, 1),
    toStartOfWeek(d, 2),
    toStartOfWeek(d, 3),
    toStartOfWeek(d, 4),
    toStartOfWeek(d, 5),
    toStartOfWeek(d, 6),
    toStartOfWeek(d, 7),
    toStartOfWeek(d, 8),
    toStartOfWeek(d, 9)
FROM t_date32 ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date32: toLastDayOfWeek';
SELECT d,
    toLastDayOfWeek(d),
    toLastDayOfWeek(d, 0),
    toLastDayOfWeek(d, 1),
    toLastDayOfWeek(d, 2),
    toLastDayOfWeek(d, 3),
    toLastDayOfWeek(d, 4),
    toLastDayOfWeek(d, 5),
    toLastDayOfWeek(d, 6),
    toLastDayOfWeek(d, 7),
    toLastDayOfWeek(d, 8),
    toLastDayOfWeek(d, 9)
FROM t_date32 ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date32: toDayOfWeek';
SELECT d,
    toDayOfWeek(d),
    toDayOfWeek(d, 0),
    toDayOfWeek(d, 1),
    toDayOfWeek(d, 2),
    toDayOfWeek(d, 3)
FROM t_date32 ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date32: toRelativeWeekNum';
SELECT d,
    toRelativeWeekNum(d)
FROM t_date32 ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date32: toStartOfInterval and date_trunc';
SELECT d,
    toStartOfInterval(d, INTERVAL 1 WEEK),
    toStartOfInterval(d, INTERVAL 2 WEEK),
    toStartOfInterval(d, INTERVAL 3 WEEK),
    toStartOfInterval(d, INTERVAL 1 WEEK, toDate32('0000-01-01')),
    toStartOfInterval(d, INTERVAL 3 WEEK, toDate32('0000-01-01')),
    date_trunc('week', d)
FROM t_date32 ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date32: dateDiff and age';
SELECT d,
    dateDiff('week', toDate32('2000-01-05'), d),
    dateDiff('week', d, toDate32('2000-01-05')),
    age('week', toDate32('2000-01-05'), d),
    age('week', d, toDate32('2000-01-05'))
FROM t_date32 ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date32: dateName and formatting';
SELECT d,
    dateName('week', d),
    dateName('weekday', d),
    formatDateTime(d, '%w %u %a'),
    formatDateTimeInJodaSyntax(d, 'e E')
FROM t_date32 ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'Date32: enable_extended_results_for_datetime_functions = 1';
SELECT d,
    toStartOfWeek(d),
    toStartOfWeek(d, 1),
    toLastDayOfWeek(d),
    toLastDayOfWeek(d, 1),
    toStartOfInterval(d, INTERVAL 1 WEEK),
    toStartOfInterval(d, INTERVAL 2 WEEK),
    date_trunc('week', d)
FROM t_date32 ORDER BY d SETTINGS enable_extended_results_for_datetime_functions = 1 FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime UTC: toWeek';
SELECT d,
    toWeek(d),
    toWeek(d, 0),
    toWeek(d, 1),
    toWeek(d, 2),
    toWeek(d, 3),
    toWeek(d, 4),
    toWeek(d, 5),
    toWeek(d, 6),
    toWeek(d, 7),
    toWeek(d, 8),
    toWeek(d, 9)
FROM t_dt_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime UTC: toYearWeek';
SELECT d,
    toYearWeek(d),
    toYearWeek(d, 0),
    toYearWeek(d, 1),
    toYearWeek(d, 2),
    toYearWeek(d, 3),
    toYearWeek(d, 4),
    toYearWeek(d, 5),
    toYearWeek(d, 6),
    toYearWeek(d, 7),
    toYearWeek(d, 8),
    toYearWeek(d, 9)
FROM t_dt_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime UTC: toStartOfWeek';
SELECT d,
    toStartOfWeek(d),
    toStartOfWeek(d, 0),
    toStartOfWeek(d, 1),
    toStartOfWeek(d, 2),
    toStartOfWeek(d, 3),
    toStartOfWeek(d, 4),
    toStartOfWeek(d, 5),
    toStartOfWeek(d, 6),
    toStartOfWeek(d, 7),
    toStartOfWeek(d, 8),
    toStartOfWeek(d, 9)
FROM t_dt_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime UTC: toLastDayOfWeek';
SELECT d,
    toLastDayOfWeek(d),
    toLastDayOfWeek(d, 0),
    toLastDayOfWeek(d, 1),
    toLastDayOfWeek(d, 2),
    toLastDayOfWeek(d, 3),
    toLastDayOfWeek(d, 4),
    toLastDayOfWeek(d, 5),
    toLastDayOfWeek(d, 6),
    toLastDayOfWeek(d, 7),
    toLastDayOfWeek(d, 8),
    toLastDayOfWeek(d, 9)
FROM t_dt_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime UTC: toDayOfWeek';
SELECT d,
    toDayOfWeek(d),
    toDayOfWeek(d, 0),
    toDayOfWeek(d, 1),
    toDayOfWeek(d, 2),
    toDayOfWeek(d, 3)
FROM t_dt_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime UTC: toRelativeWeekNum';
SELECT d,
    toRelativeWeekNum(d)
FROM t_dt_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime UTC: toStartOfInterval and date_trunc';
SELECT d,
    toStartOfInterval(d, INTERVAL 1 WEEK),
    toStartOfInterval(d, INTERVAL 2 WEEK),
    toStartOfInterval(d, INTERVAL 3 WEEK),
    toStartOfInterval(d, INTERVAL 1 WEEK, toDateTime('1970-01-01 00:00:00', 'UTC')),
    toStartOfInterval(d, INTERVAL 3 WEEK, toDateTime('1970-01-01 00:00:00', 'UTC')),
    date_trunc('week', d)
FROM t_dt_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime UTC: dateDiff and age';
SELECT d,
    dateDiff('week', toDateTime('2000-01-05 00:00:00', 'UTC'), d),
    dateDiff('week', d, toDateTime('2000-01-05 00:00:00', 'UTC')),
    age('week', toDateTime('2000-01-05 00:00:00', 'UTC'), d),
    age('week', d, toDateTime('2000-01-05 00:00:00', 'UTC'))
FROM t_dt_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime UTC: dateName and formatting';
SELECT d,
    dateName('week', d),
    dateName('weekday', d),
    formatDateTime(d, '%w %u %a'),
    formatDateTimeInJodaSyntax(d, 'e E'),
    fromUnixTimestamp(toUnixTimestamp(d), '%w %u', 'UTC')
FROM t_dt_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime64 UTC: toWeek';
SELECT d,
    toWeek(d),
    toWeek(d, 0),
    toWeek(d, 1),
    toWeek(d, 2),
    toWeek(d, 3),
    toWeek(d, 4),
    toWeek(d, 5),
    toWeek(d, 6),
    toWeek(d, 7),
    toWeek(d, 8),
    toWeek(d, 9)
FROM t_dt64_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime64 UTC: toYearWeek';
SELECT d,
    toYearWeek(d),
    toYearWeek(d, 0),
    toYearWeek(d, 1),
    toYearWeek(d, 2),
    toYearWeek(d, 3),
    toYearWeek(d, 4),
    toYearWeek(d, 5),
    toYearWeek(d, 6),
    toYearWeek(d, 7),
    toYearWeek(d, 8),
    toYearWeek(d, 9)
FROM t_dt64_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime64 UTC: toStartOfWeek';
SELECT d,
    toStartOfWeek(d),
    toStartOfWeek(d, 0),
    toStartOfWeek(d, 1),
    toStartOfWeek(d, 2),
    toStartOfWeek(d, 3),
    toStartOfWeek(d, 4),
    toStartOfWeek(d, 5),
    toStartOfWeek(d, 6),
    toStartOfWeek(d, 7),
    toStartOfWeek(d, 8),
    toStartOfWeek(d, 9)
FROM t_dt64_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime64 UTC: toLastDayOfWeek';
SELECT d,
    toLastDayOfWeek(d),
    toLastDayOfWeek(d, 0),
    toLastDayOfWeek(d, 1),
    toLastDayOfWeek(d, 2),
    toLastDayOfWeek(d, 3),
    toLastDayOfWeek(d, 4),
    toLastDayOfWeek(d, 5),
    toLastDayOfWeek(d, 6),
    toLastDayOfWeek(d, 7),
    toLastDayOfWeek(d, 8),
    toLastDayOfWeek(d, 9)
FROM t_dt64_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime64 UTC: toDayOfWeek';
SELECT d,
    toDayOfWeek(d),
    toDayOfWeek(d, 0),
    toDayOfWeek(d, 1),
    toDayOfWeek(d, 2),
    toDayOfWeek(d, 3)
FROM t_dt64_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime64 UTC: toRelativeWeekNum';
SELECT d,
    toRelativeWeekNum(d)
FROM t_dt64_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime64 UTC: toStartOfInterval and date_trunc';
SELECT d,
    toStartOfInterval(d, INTERVAL 1 WEEK),
    toStartOfInterval(d, INTERVAL 2 WEEK),
    toStartOfInterval(d, INTERVAL 3 WEEK),
    toStartOfInterval(d, INTERVAL 1 WEEK, fromUnixTimestamp64Milli(-62167219200000, 'UTC')),
    toStartOfInterval(d, INTERVAL 3 WEEK, fromUnixTimestamp64Milli(-62167219200000, 'UTC')),
    date_trunc('week', d)
FROM t_dt64_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime64 UTC: dateDiff and age';
SELECT d,
    dateDiff('week', toDateTime64('2000-01-05 00:00:00', 3, 'UTC'), d),
    dateDiff('week', d, toDateTime64('2000-01-05 00:00:00', 3, 'UTC')),
    age('week', toDateTime64('2000-01-05 00:00:00', 3, 'UTC'), d),
    age('week', d, toDateTime64('2000-01-05 00:00:00', 3, 'UTC'))
FROM t_dt64_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime64 UTC: dateName and formatting';
SELECT d,
    dateName('week', d),
    dateName('weekday', d),
    formatDateTime(d, '%w %u %a'),
    formatDateTimeInJodaSyntax(d, 'e E')
FROM t_dt64_utc ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime64 UTC: enable_extended_results_for_datetime_functions = 1';
SELECT d,
    toStartOfWeek(d),
    toStartOfWeek(d, 1),
    toLastDayOfWeek(d),
    toLastDayOfWeek(d, 1),
    toStartOfInterval(d, INTERVAL 1 WEEK),
    toStartOfInterval(d, INTERVAL 2 WEEK),
    date_trunc('week', d)
FROM t_dt64_utc ORDER BY d SETTINGS enable_extended_results_for_datetime_functions = 1 FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime Asia/Kathmandu: toWeek';
SELECT d,
    toWeek(d),
    toWeek(d, 0),
    toWeek(d, 1),
    toWeek(d, 2),
    toWeek(d, 3),
    toWeek(d, 4),
    toWeek(d, 5),
    toWeek(d, 6),
    toWeek(d, 7),
    toWeek(d, 8),
    toWeek(d, 9)
FROM t_dt_kathmandu ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime Asia/Kathmandu: toYearWeek';
SELECT d,
    toYearWeek(d),
    toYearWeek(d, 0),
    toYearWeek(d, 1),
    toYearWeek(d, 2),
    toYearWeek(d, 3),
    toYearWeek(d, 4),
    toYearWeek(d, 5),
    toYearWeek(d, 6),
    toYearWeek(d, 7),
    toYearWeek(d, 8),
    toYearWeek(d, 9)
FROM t_dt_kathmandu ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime Asia/Kathmandu: toStartOfWeek';
SELECT d,
    toStartOfWeek(d),
    toStartOfWeek(d, 0),
    toStartOfWeek(d, 1),
    toStartOfWeek(d, 2),
    toStartOfWeek(d, 3),
    toStartOfWeek(d, 4),
    toStartOfWeek(d, 5),
    toStartOfWeek(d, 6),
    toStartOfWeek(d, 7),
    toStartOfWeek(d, 8),
    toStartOfWeek(d, 9)
FROM t_dt_kathmandu ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime Asia/Kathmandu: toLastDayOfWeek';
SELECT d,
    toLastDayOfWeek(d),
    toLastDayOfWeek(d, 0),
    toLastDayOfWeek(d, 1),
    toLastDayOfWeek(d, 2),
    toLastDayOfWeek(d, 3),
    toLastDayOfWeek(d, 4),
    toLastDayOfWeek(d, 5),
    toLastDayOfWeek(d, 6),
    toLastDayOfWeek(d, 7),
    toLastDayOfWeek(d, 8),
    toLastDayOfWeek(d, 9)
FROM t_dt_kathmandu ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime Asia/Kathmandu: toDayOfWeek';
SELECT d,
    toDayOfWeek(d),
    toDayOfWeek(d, 0),
    toDayOfWeek(d, 1),
    toDayOfWeek(d, 2),
    toDayOfWeek(d, 3)
FROM t_dt_kathmandu ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime Asia/Kathmandu: toRelativeWeekNum';
SELECT d,
    toRelativeWeekNum(d)
FROM t_dt_kathmandu ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime Asia/Kathmandu: toStartOfInterval and date_trunc';
SELECT d,
    toStartOfInterval(d, INTERVAL 1 WEEK),
    toStartOfInterval(d, INTERVAL 2 WEEK),
    toStartOfInterval(d, INTERVAL 3 WEEK),
    toStartOfInterval(d, INTERVAL 1 WEEK, toDateTime('2000-01-06 00:00:00', 'Asia/Kathmandu')),
    toStartOfInterval(d, INTERVAL 3 WEEK, toDateTime('2000-01-06 00:00:00', 'Asia/Kathmandu')),
    date_trunc('week', d)
FROM t_dt_kathmandu ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime Asia/Kathmandu: dateDiff and age';
SELECT d,
    dateDiff('week', toDateTime('2000-01-05 00:00:00', 'Asia/Kathmandu'), d),
    dateDiff('week', d, toDateTime('2000-01-05 00:00:00', 'Asia/Kathmandu')),
    age('week', toDateTime('2000-01-05 00:00:00', 'Asia/Kathmandu'), d),
    age('week', d, toDateTime('2000-01-05 00:00:00', 'Asia/Kathmandu'))
FROM t_dt_kathmandu ORDER BY d FORMAT TSVWithNamesAndTypes;

SELECT 'DateTime Asia/Kathmandu: dateName and formatting';
SELECT d,
    dateName('week', d),
    dateName('weekday', d),
    formatDateTime(d, '%w %u %a'),
    formatDateTimeInJodaSyntax(d, 'e E'),
    fromUnixTimestamp(toUnixTimestamp(d), '%w %u', 'Asia/Kathmandu')
FROM t_dt_kathmandu ORDER BY d FORMAT TSVWithNamesAndTypes;

-- `String` arguments take a separate branch that parses the value before the week calculation.
SELECT 'String arguments';
SELECT d,
    toWeek(d),
    toWeek(d, 3),
    toYearWeek(d),
    toYearWeek(d, 3),
    toDayOfWeek(d),
    toDayOfWeek(d, 2)
FROM t_string ORDER BY d FORMAT TSVWithNamesAndTypes;

-- The optional timezone argument, which comes after the mode: 00:00 in Kathmandu is the previous day in UTC.
SELECT 'Timezone argument';
SELECT d,
    toWeek(d, 0, 'UTC'),
    toYearWeek(d, 0, 'UTC'),
    toStartOfWeek(d, 0, 'UTC'),
    toLastDayOfWeek(d, 0, 'UTC'),
    toDayOfWeek(d, 0, 'UTC'),
    toWeek(d, 1, 'UTC'),
    toYearWeek(d, 1, 'UTC'),
    toStartOfWeek(d, 1, 'UTC'),
    toLastDayOfWeek(d, 1, 'UTC'),
    toDayOfWeek(d, 1, 'UTC')
FROM t_dt_kathmandu ORDER BY d FORMAT TSVWithNamesAndTypes;

-- Translations from other SQL dialects to `toDayOfWeek`. The aliases keep the column names independent of
-- the function call that `EXTRACT` is translated to.
SELECT 'EXTRACT';
SELECT d,
    EXTRACT(ISODOW FROM d) AS isodow,
    EXTRACT(DOW FROM d) AS dow
FROM t_date WHERE d BETWEEN '2026-12-27' AND '2027-01-05' ORDER BY d FORMAT TSVWithNamesAndTypes;

DROP TABLE t_date;
DROP TABLE t_date32;
DROP TABLE t_dt_utc;
DROP TABLE t_dt64_utc;
DROP TABLE t_dt_kathmandu;
DROP TABLE t_string;

-- Saturday, Sunday, Monday.
SET enable_trino_dialect = 1;
SET dialect = 'trino';
SELECT day_of_week(DATE '2026-01-03'), day_of_week(DATE '2026-01-04'), day_of_week(DATE '2026-01-05'), dow(DATE '2026-01-03'), dow(DATE '2026-01-04'), dow(DATE '2026-01-05');
SET dialect = 'clickhouse';

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';
print dayofweek(datetime(2026-01-03)), dayofweek(datetime(2026-01-04)), dayofweek(datetime(2026-01-05));
SET dialect = 'clickhouse';
