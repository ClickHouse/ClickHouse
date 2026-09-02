-- Tests that functions `toDayOfWeek()`, 'toWeek()' and 'toYearWeek()' accepts a date given as string (for compatibility with MySQL)

SELECT '-- Constant argument';

SELECT toDayOfWeek(toDateTime('2016-06-15 23:00:00')), toDayOfWeek('2016-06-15'), toDayOfWeek('2016-06-15 23:00:00'), toDayOfWeek('2016-06-15 23:00:00.123456');
SELECT toWeek(toDateTime('2016-06-15 23:00:00')), toWeek('2016-06-15'), toWeek('2016-06-15 23:00:00'), toWeek('2016-06-15 23:00:00.123456');
SELECT toYearWeek(toDateTime('2016-06-15 23:00:00')), toYearWeek('2016-06-15'), toYearWeek('2016-06-15 23:00:00'), toYearWeek('2016-06-15 23:00:00.123456');

SELECT toDayOfWeek('invalid'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toWeek('invalid'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT toYearWeek('invalid'); -- { serverError CANNOT_PARSE_DATETIME }

SELECT '-- Non-constant argument';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    d        Date,
    dt       DateTime('UTC'),
    dt64     DateTime64(6, 'UTC'),
    str_d    String,
    str_dt   String,
    str_dt64 String,
    invalid  String
) ENGINE MergeTree ORDER BY dt;

INSERT INTO `tab` VALUES (toDate('2017-09-13'), toDateTime('2017-09-13 19:10:22', 'UTC'), toDateTime64('2017-09-13 19:10:22.123456', 6, 'UTC'), '2017-09-13', '2017-09-13 19:10:22', '2017-09-13 19:10:22.123456', 'foo');
INSERT INTO `tab` VALUES (toDate('2017-09-24'), toDateTime('2017-09-24 12:05:34', 'UTC'), toDateTime64('2017-09-24 12:05:34.123456', 6, 'UTC'), '2017-09-24', '2017-09-24 12:05:34', '2017-09-24 12:05:34.123456', 'bar');
INSERT INTO `tab` VALUES (toDate('2018-01-29'), toDateTime('2018-01-29 02:09:48', 'UTC'), toDateTime64('2018-01-29 02:09:48.123456', 6, 'UTC'), '2018-01-29', '2018-01-29 02:09:48', '2018-01-29 02:09:48.123456', 'qaz');
INSERT INTO `tab` VALUES (toDate('2019-02-21'), toDateTime('2019-02-21 15:07:43', 'UTC'), toDateTime64('2019-02-21 15:07:43.123456', 6, 'UTC'), '2019-02-21', '2019-02-21 15:07:43', '2019-02-21 15:07:43.123456', 'qux');

SELECT toDayOfWeek(d), toDayOfWeek(dt), toDayOfWeek(dt64), toDayOfWeek(str_d), toDayOfWeek(str_dt), toDayOfWeek(str_dt64) FROM tab ORDER BY d;
SELECT toWeek(d), toWeek(dt), toWeek(dt64), toWeek(str_d), toWeek(str_dt), toWeek(str_dt64) FROM tab ORDER BY d;
SELECT toYearWeek(d), toYearWeek(dt), toYearWeek(dt64), toYearWeek(str_d), toYearWeek(str_dt), toYearWeek(str_dt64) FROM tab ORDER BY d;

SELECT toDayOfWeek(invalid) FROM `tab`; -- { serverError CANNOT_PARSE_DATETIME }
SELECT toWeek(invalid) FROM `tab`; -- { serverError CANNOT_PARSE_DATETIME }
SELECT toYearWeek(invalid) FROM `tab`; -- { serverError CANNOT_PARSE_DATETIME }

DROP TABLE tab;
