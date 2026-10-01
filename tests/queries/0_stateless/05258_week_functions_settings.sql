-- The `week_functions_starting_day`, `week_functions_first_week_of_year` and `week_functions_range` settings. They
-- change the week functions called without `mode` (or, for `toStartOfInterval`, without `origin`). With the default
-- 'auto', each function keeps its own historic default. The first section shows examples, the others check each part
-- of the behaviour. Fixed expected values were computed from the definitions, independently of the implementation.

SET session_timezone = 'UTC';
SET output_format_pretty_row_numbers = 0;

DROP VIEW IF EXISTS v_explicit;
DROP VIEW IF EXISTS v_agreement;
DROP VIEW IF EXISTS v_values;
DROP VIEW IF EXISTS v_ends;
DROP VIEW IF EXISTS v_outside;
DROP VIEW IF EXISTS v_table_end;
DROP VIEW IF EXISTS v_saturday;
DROP TABLE IF EXISTS t_examples;
DROP TABLE IF EXISTS t_new_year;
DROP TABLE IF EXISTS t_days;
DROP TABLE IF EXISTS t_expected;
DROP TABLE IF EXISTS t_saturday;
DROP TABLE IF EXISTS t_week_key;
DROP TABLE IF EXISTS t_week_cache;
DROP TABLE IF EXISTS t_week_partition;
DROP TABLE IF EXISTS t_week_sorting;
DROP TABLE IF EXISTS t_week_default;

SELECT '-- examples';

-- Saturday 2026-01-03 to Saturday 2026-01-10.
CREATE TABLE t_examples (d Date) ENGINE = Memory;
INSERT INTO t_examples SELECT toDate('2026-01-03') + number FROM numbers(8);

-- `toStartOfWeek`, `toLastDayOfWeek` and `%w` start weeks on Sunday, the other functions on Monday.
SELECT 'By default, some functions start weeks on Sunday, the others on Monday';
SELECT
    d,
    toDayOfWeek(d),
    formatDateTime(d, '%w') AS `%w`,
    toStartOfWeek(d),
    toLastDayOfWeek(d),
    toStartOfInterval(d, INTERVAL 1 WEEK) AS toStartOfInterval,
    date_trunc('week', d) AS date_trunc,
    toRelativeWeekNum(d),
    dateDiff('week', toDate('2026-01-03'), d) AS dateDiff
FROM t_examples ORDER BY d
FORMAT PrettyCompactMonoBlock;

SELECT 'With week_functions_starting_day = saturday, all of them start weeks on Saturday';
SELECT
    d,
    toDayOfWeek(d),
    formatDateTime(d, '%w') AS `%w`,
    toStartOfWeek(d),
    toLastDayOfWeek(d),
    toStartOfInterval(d, INTERVAL 1 WEEK) AS toStartOfInterval,
    date_trunc('week', d) AS date_trunc,
    toRelativeWeekNum(d),
    dateDiff('week', toDate('2026-01-03'), d) AS dateDiff
FROM t_examples ORDER BY d
SETTINGS week_functions_starting_day = 'saturday'
FORMAT PrettyCompactMonoBlock;

-- In weeks from Monday, the week with Thursday 2026-01-01 has 4 days in 2026, and the week with Friday 2027-01-01 has
-- 3 days in 2027. Week 1 is the first full week, the first week with 4 or more days, or the week with January 1. With
-- 0-53, the days before week 1 are in week 0; with 1-53, they're in the last week of the previous year.
CREATE TABLE t_new_year (d Date) ENGINE = Memory;
INSERT INTO t_new_year VALUES ('2025-12-29'), ('2026-01-01'), ('2026-01-05'), ('2026-12-31'), ('2027-01-01'), ('2027-01-04');

SELECT 'The week of the year: toWeek and toYearWeek, in weeks from Monday';
SELECT 'dates', groupArray(d) FROM (SELECT d FROM t_new_year ORDER BY d);
SELECT 'first_full_week 0-53', groupArray(toWeek(d)), groupArray(toYearWeek(d)) FROM (SELECT d FROM t_new_year ORDER BY d)
SETTINGS week_functions_starting_day = 'monday', week_functions_first_week_of_year = 'first_full_week', week_functions_range = '0-53';
SELECT 'first_full_week 1-53', groupArray(toWeek(d)), groupArray(toYearWeek(d)) FROM (SELECT d FROM t_new_year ORDER BY d)
SETTINGS week_functions_starting_day = 'monday', week_functions_first_week_of_year = 'first_full_week', week_functions_range = '1-53';
SELECT 'four_or_more_days 0-53', groupArray(toWeek(d)), groupArray(toYearWeek(d)) FROM (SELECT d FROM t_new_year ORDER BY d)
SETTINGS week_functions_starting_day = 'monday', week_functions_first_week_of_year = 'four_or_more_days', week_functions_range = '0-53';
SELECT 'four_or_more_days 1-53', groupArray(toWeek(d)), groupArray(toYearWeek(d)) FROM (SELECT d FROM t_new_year ORDER BY d)
SETTINGS week_functions_starting_day = 'monday', week_functions_first_week_of_year = 'four_or_more_days', week_functions_range = '1-53';
SELECT 'contains_january_1', groupArray(toWeek(d)), groupArray(toYearWeek(d)) FROM (SELECT d FROM t_new_year ORDER BY d)
SETTINGS week_functions_starting_day = 'monday', week_functions_first_week_of_year = 'contains_january_1';

-- `dateName('week')` is the ISO week (Monday, 1-53, `four_or_more_days`), and each setting replaces its part of it.
SELECT 'dateName(week): the ISO week by default, and with first_full_week';
SELECT 'auto', groupArray(dateName('week', d)), groupArray(toISOWeek(d)) FROM (SELECT d FROM t_new_year ORDER BY d);
SELECT 'first_full_week', groupArray(dateName('week', d)) FROM (SELECT d FROM t_new_year ORDER BY d)
SETTINGS week_functions_first_week_of_year = 'first_full_week';

SELECT 'An explicit mode or origin ignores the settings';
SELECT
    toDayOfWeek(d),
    toDayOfWeek(d, 0),
    toStartOfWeek(d),
    toStartOfWeek(d, 0),
    toStartOfInterval(d, INTERVAL 1 WEEK) AS toStartOfInterval,
    toStartOfInterval(d, INTERVAL 1 WEEK, toDate('2025-12-29')) AS with_origin
FROM (SELECT toDate('2026-01-04') AS d)
SETTINGS week_functions_starting_day = 'saturday'
FORMAT PrettyCompactMonoBlock;

SELECT 'Not affected: ISO weeks, age, formatDateTime %u, EXTRACT(ISODOW) and EXTRACT(DOW)';
SELECT
    toISOWeek(d),
    age('week', d, d + 7) AS age,
    formatDateTime(d, '%u') AS `%u`,
    EXTRACT(ISODOW FROM d) AS isodow,
    EXTRACT(DOW FROM d) AS dow
FROM (SELECT toDate('2026-01-04') AS d)
SETTINGS week_functions_starting_day = 'saturday', week_functions_first_week_of_year = 'contains_january_1', week_functions_range = '0-53'
FORMAT PrettyCompactMonoBlock;

SELECT '-- values';
SELECT name, value, default FROM system.settings WHERE name LIKE 'week\_functions\_%' ORDER BY name;
SET week_functions_starting_day = 'saturday', week_functions_range = '1-53', week_functions_first_week_of_year = 'contains_january_1';
SELECT name, value FROM system.settings WHERE name LIKE 'week\_functions\_%' ORDER BY name;
SET week_functions_starting_day = 'someday'; -- { serverError BAD_ARGUMENTS }
SET week_functions_range = '0-54'; -- { serverError BAD_ARGUMENTS }
SET week_functions_first_week_of_year = 'iso'; -- { serverError BAD_ARGUMENTS }
SELECT 1 SETTINGS week_functions_starting_day = 'someday'; -- { clientError BAD_ARGUMENTS }
-- A failed `SET` leaves the previous values.
SELECT name, value FROM system.settings WHERE name LIKE 'week\_functions\_%' ORDER BY name;
SET week_functions_starting_day = 'auto', week_functions_range = 'auto', week_functions_first_week_of_year = 'auto';

-- The days of the checks below: 2016-12-27 to 2017-01-05 and 2018-12-27 to 2019-01-05, at noon for the types with
-- time. Around these two New Years, every `mode`, and every first week rule and range for every start day, give
-- different results.
CREATE TABLE t_days (d Date, d32 Date32, dt DateTime('UTC'), dt64 DateTime64(3, 'UTC')) ENGINE = Memory;
INSERT INTO t_days SELECT toDate(day), day, toDateTime(day, 'UTC') + INTERVAL 12 HOUR, toDateTime64(day, 3, 'UTC') + INTERVAL 43200500 MILLISECOND
FROM (SELECT toDate32('2016-12-27') + number AS day FROM numbers(10) UNION ALL SELECT toDate32('2018-12-27') + number FROM numbers(10));

SELECT '-- the settings that describe each mode';
-- Without `mode`, the week functions under the settings that describe `mode` n give the same results as with `mode` n.
-- Each row is the number of days where they differ.
SELECT 'mode 0', countIf((toWeek(d), toYearWeek(d), toStartOfWeek(d), toLastDayOfWeek(d)) != (toWeek(d, 0), toYearWeek(d, 0), toStartOfWeek(d, 0), toLastDayOfWeek(d, 0)))
FROM t_days SETTINGS week_functions_starting_day = 'sunday', week_functions_range = '0-53', week_functions_first_week_of_year = 'first_full_week';
SELECT 'mode 1', countIf((toWeek(d), toYearWeek(d), toStartOfWeek(d), toLastDayOfWeek(d)) != (toWeek(d, 1), toYearWeek(d, 1), toStartOfWeek(d, 1), toLastDayOfWeek(d, 1)))
FROM t_days SETTINGS week_functions_starting_day = 'monday', week_functions_range = '0-53', week_functions_first_week_of_year = 'four_or_more_days';
SELECT 'mode 2', countIf((toWeek(d), toYearWeek(d), toStartOfWeek(d), toLastDayOfWeek(d)) != (toWeek(d, 2), toYearWeek(d, 2), toStartOfWeek(d, 2), toLastDayOfWeek(d, 2)))
FROM t_days SETTINGS week_functions_starting_day = 'sunday', week_functions_range = '1-53', week_functions_first_week_of_year = 'first_full_week';
SELECT 'mode 3', countIf((toWeek(d), toYearWeek(d), toStartOfWeek(d), toLastDayOfWeek(d)) != (toWeek(d, 3), toYearWeek(d, 3), toStartOfWeek(d, 3), toLastDayOfWeek(d, 3)))
FROM t_days SETTINGS week_functions_starting_day = 'monday', week_functions_range = '1-53', week_functions_first_week_of_year = 'four_or_more_days';
SELECT 'mode 4', countIf((toWeek(d), toYearWeek(d), toStartOfWeek(d), toLastDayOfWeek(d)) != (toWeek(d, 4), toYearWeek(d, 4), toStartOfWeek(d, 4), toLastDayOfWeek(d, 4)))
FROM t_days SETTINGS week_functions_starting_day = 'sunday', week_functions_range = '0-53', week_functions_first_week_of_year = 'four_or_more_days';
SELECT 'mode 5', countIf((toWeek(d), toYearWeek(d), toStartOfWeek(d), toLastDayOfWeek(d)) != (toWeek(d, 5), toYearWeek(d, 5), toStartOfWeek(d, 5), toLastDayOfWeek(d, 5)))
FROM t_days SETTINGS week_functions_starting_day = 'monday', week_functions_range = '0-53', week_functions_first_week_of_year = 'first_full_week';
SELECT 'mode 6', countIf((toWeek(d), toYearWeek(d), toStartOfWeek(d), toLastDayOfWeek(d)) != (toWeek(d, 6), toYearWeek(d, 6), toStartOfWeek(d, 6), toLastDayOfWeek(d, 6)))
FROM t_days SETTINGS week_functions_starting_day = 'sunday', week_functions_range = '1-53', week_functions_first_week_of_year = 'four_or_more_days';
SELECT 'mode 7', countIf((toWeek(d), toYearWeek(d), toStartOfWeek(d), toLastDayOfWeek(d)) != (toWeek(d, 7), toYearWeek(d, 7), toStartOfWeek(d, 7), toLastDayOfWeek(d, 7)))
FROM t_days SETTINGS week_functions_starting_day = 'monday', week_functions_range = '1-53', week_functions_first_week_of_year = 'first_full_week';
SELECT 'mode 8', countIf((toWeek(d), toYearWeek(d), toStartOfWeek(d), toLastDayOfWeek(d)) != (toWeek(d, 8), toYearWeek(d, 8), toStartOfWeek(d, 8), toLastDayOfWeek(d, 8)))
FROM t_days SETTINGS week_functions_starting_day = 'sunday', week_functions_range = '1-53', week_functions_first_week_of_year = 'contains_january_1';
SELECT 'mode 9', countIf((toWeek(d), toYearWeek(d), toStartOfWeek(d), toLastDayOfWeek(d)) != (toWeek(d, 9), toYearWeek(d, 9), toStartOfWeek(d, 9), toLastDayOfWeek(d, 9)))
FROM t_days SETTINGS week_functions_starting_day = 'monday', week_functions_range = '1-53', week_functions_first_week_of_year = 'contains_january_1';
-- `toDayOfWeek` numbers the days from 1 in modes 0 and 3.
SELECT 'toDayOfWeek mode 0', countIf(toDayOfWeek(d) != toDayOfWeek(d, 0)) FROM t_days SETTINGS week_functions_starting_day = 'monday';
SELECT 'toDayOfWeek mode 3', countIf(toDayOfWeek(d) != toDayOfWeek(d, 3)) FROM t_days SETTINGS week_functions_starting_day = 'sunday';

SELECT '-- an explicit mode or origin, and the functions that do not depend on the settings';
-- `results` are the functions the settings must not affect: every explicit `mode`, `toStartOfInterval` with an `origin`
-- (Monday 2016-12-26), and the functions that never read the settings. `control` is `toStartOfWeek` without `mode`,
-- which the settings do affect. `t_expected` stores both as computed with the settings at default 'auto'. The view is
-- computed again by the second SELECT statement under its own settings, which change every part of the default mode: the
-- `results` must stay the same, and the `control` must differ on every day (its weeks start on Saturday instead of
-- Sunday), which proves the second computation used the new settings.
CREATE VIEW v_explicit AS SELECT d, (
    toWeek(d, 0), toWeek(d, 1), toWeek(d, 2), toWeek(d, 3), toWeek(d, 4), toWeek(d, 5), toWeek(d, 6), toWeek(d, 7), toWeek(d, 8), toWeek(d, 9),
    toYearWeek(d, 0), toYearWeek(d, 1), toYearWeek(d, 2), toYearWeek(d, 3), toYearWeek(d, 4), toYearWeek(d, 5), toYearWeek(d, 6), toYearWeek(d, 7), toYearWeek(d, 8), toYearWeek(d, 9),
    toStartOfWeek(d, 0), toStartOfWeek(d, 1), toStartOfWeek(d, 2), toStartOfWeek(d, 3), toStartOfWeek(d, 4), toStartOfWeek(d, 5), toStartOfWeek(d, 6), toStartOfWeek(d, 7), toStartOfWeek(d, 8), toStartOfWeek(d, 9),
    toLastDayOfWeek(d, 0), toLastDayOfWeek(d, 1), toLastDayOfWeek(d, 2), toLastDayOfWeek(d, 3), toLastDayOfWeek(d, 4), toLastDayOfWeek(d, 5), toLastDayOfWeek(d, 6), toLastDayOfWeek(d, 7), toLastDayOfWeek(d, 8), toLastDayOfWeek(d, 9),
    toDayOfWeek(d, 0), toDayOfWeek(d, 1), toDayOfWeek(d, 2), toDayOfWeek(d, 3),
    toStartOfInterval(d, INTERVAL 1 WEEK, toDate('2016-12-26')), toStartOfInterval(d, INTERVAL 2 WEEK, toDate('2016-12-26')),
    toISOWeek(d), toISOYear(d), toMonday(d), age('week', toDate('2016-12-27'), d), timeDiff(toDate('2016-12-27'), d),
    dateName('weekday', d), formatDateTime(d, '%u %V %G %g %a %W %j'), formatDateTimeInJodaSyntax(d, 'e E x w'),
    fromUnixTimestamp(toUnixTimestamp(dt), '%u %V %a %W', 'UTC')) AS results,
    toStartOfWeek(d) AS control
FROM t_days;
CREATE TABLE t_expected ENGINE = Memory AS SELECT * FROM v_explicit;
SELECT count(), countIf(v_explicit.results != t_expected.results), countIf(v_explicit.control != t_expected.control)
FROM v_explicit INNER JOIN t_expected USING (d)
SETTINGS week_functions_starting_day = 'saturday', week_functions_range = '1-53', week_functions_first_week_of_year = 'contains_january_1';

SELECT '-- translations';
-- `EXTRACT(ISODOW)`, `EXTRACT(DOW)`, Trino `day_of_week`/`dow` and Kusto `dayofweek` are translated to `toDayOfWeek`
-- with an explicit `mode`, so `week_functions_starting_day` doesn't change them. The days are Saturday 2026-01-03,
-- Sunday 2026-01-04 and Monday 2026-01-05.

-- The column names show the explicit mode.
SELECT EXTRACT(ISODOW FROM d), EXTRACT(DOW FROM d) FROM (SELECT toDate('2026-01-03') AS d) FORMAT TSVWithNames;

SELECT d, EXTRACT(ISODOW FROM d), EXTRACT(DOW FROM d)
FROM (SELECT arrayJoin([toDate('2026-01-03'), toDate('2026-01-04'), toDate('2026-01-05')]) AS d) ORDER BY d
SETTINGS week_functions_starting_day = 'saturday';

SET week_functions_starting_day = 'saturday';
SET enable_trino_dialect = 1;
SET dialect = 'trino';
SELECT day_of_week(DATE '2026-01-03'), day_of_week(DATE '2026-01-04'), day_of_week(DATE '2026-01-05'), dow(DATE '2026-01-03'), dow(DATE '2026-01-04'), dow(DATE '2026-01-05');
SET dialect = 'clickhouse';

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';
print dayofweek(datetime(2026-01-03)), dayofweek(datetime(2026-01-04)), dayofweek(datetime(2026-01-05));
-- Kusto `datetime_diff('week')` is `dateDiff('week')`, so it follows the setting: from Friday to Saturday is one week.
print datetime_diff('week', datetime(2026-01-03), datetime(2026-01-02));
SET dialect = 'clickhouse';

-- Without a `mode`, `toDayOfWeek` follows the setting.
SELECT toDayOfWeek(toDate('2026-01-03'));
SET week_functions_starting_day = 'auto';

SELECT '-- every start day';
-- A view is expanded in each query that reads it, so the functions in it follow the settings of that query.

-- The first day of the week that `toStartOfWeek` returns (Monday = 1), then, for each other function, the number
-- of days where it disagrees.
CREATE VIEW v_agreement AS SELECT
    groupUniqArray(toDayOfWeek(toStartOfWeek(d), 0)) AS first_day,
    countIf(toStartOfWeek(d) + toDayOfWeek(d) - 1 != d) AS toDayOfWeek,
    countIf(toLastDayOfWeek(d) != toStartOfWeek(d) + 6) AS toLastDayOfWeek,
    countIf(toStartOfInterval(d, INTERVAL 1 WEEK) != toStartOfWeek(d)) AS toStartOfInterval,
    countIf(date_trunc('week', d) != toStartOfWeek(d)) AS date_trunc,
    countIf(toRelativeWeekNum(d) - toRelativeWeekNum(d - 1) != (toDayOfWeek(d) = 1)) AS toRelativeWeekNum,
    countIf(dateDiff('week', toDate('2016-12-27'), d) != toRelativeWeekNum(d) - toRelativeWeekNum(toDate('2016-12-27'))) AS dateDiff,
    countIf(formatDateTime(d, '%w') != toString(toDayOfWeek(d) - 1)) AS `%w`
FROM t_days;

SELECT 'monday' AS starting_day, * FROM v_agreement SETTINGS week_functions_starting_day = 'monday' FORMAT TSVWithNames;
SELECT 'tuesday', * FROM v_agreement SETTINGS week_functions_starting_day = 'tuesday';
SELECT 'wednesday', * FROM v_agreement SETTINGS week_functions_starting_day = 'wednesday';
SELECT 'thursday', * FROM v_agreement SETTINGS week_functions_starting_day = 'thursday';
SELECT 'friday', * FROM v_agreement SETTINGS week_functions_starting_day = 'friday';
SELECT 'saturday', * FROM v_agreement SETTINGS week_functions_starting_day = 'saturday';
SELECT 'sunday', * FROM v_agreement SETTINGS week_functions_starting_day = 'sunday';

-- Intervals of 1, 2 and 3 weeks that contain Friday 2026-01-02: they are counted from the first start day on or after
-- 1970-01-01. Then the week numbers from Friday 2026-01-02 to Monday 2026-01-05.
CREATE VIEW v_values AS SELECT
    toStartOfInterval(toDate('2026-01-02'), INTERVAL 1 WEEK) AS week_1,
    toStartOfInterval(toDate('2026-01-02'), INTERVAL 2 WEEK) AS week_2,
    toStartOfInterval(toDate('2026-01-02'), INTERVAL 3 WEEK) AS week_3,
    [toRelativeWeekNum(toDate('2026-01-02')), toRelativeWeekNum(toDate('2026-01-03')), toRelativeWeekNum(toDate('2026-01-04')), toRelativeWeekNum(toDate('2026-01-05'))] AS toRelativeWeekNum;

SELECT 'monday' AS starting_day, * FROM v_values SETTINGS week_functions_starting_day = 'monday' FORMAT TSVWithNames;
SELECT 'tuesday', * FROM v_values SETTINGS week_functions_starting_day = 'tuesday';
SELECT 'wednesday', * FROM v_values SETTINGS week_functions_starting_day = 'wednesday';
SELECT 'thursday', * FROM v_values SETTINGS week_functions_starting_day = 'thursday';
SELECT 'friday', * FROM v_values SETTINGS week_functions_starting_day = 'friday';
SELECT 'saturday', * FROM v_values SETTINGS week_functions_starting_day = 'saturday';
SELECT 'sunday', * FROM v_values SETTINGS week_functions_starting_day = 'sunday';

-- The ends of the ranges of `Date` and `Date32`: the week of 1970-01-01 starts before the range of `Date` unless weeks
-- start on Thursday, so `toStartOfWeek` saturates. With extended results, `Date32` goes past its range within the
-- lookup table (1900-2299) and saturates at 0000-01-01 and 9999-12-31. The views are created with extended results,
-- because the types of their columns are set when they are created.
SET enable_extended_results_for_datetime_functions = 1;
CREATE VIEW v_ends AS SELECT
    toStartOfWeek(toDate('1970-01-01')) AS date_start,
    toLastDayOfWeek(toDate('2149-06-06')) AS date_last,
    toStartOfWeek(toDate32('1900-01-01')) AS date32_start,
    toLastDayOfWeek(toDate32('2299-12-31')) AS date32_last,
    toStartOfWeek(toDate32('0000-01-01')) AS date32_min_start,
    toLastDayOfWeek(toDate32('9999-12-31')) AS date32_max_last;

SELECT 'monday' AS starting_day, * FROM v_ends SETTINGS week_functions_starting_day = 'monday' FORMAT TSVWithNames;
SELECT 'tuesday', * FROM v_ends SETTINGS week_functions_starting_day = 'tuesday';
SELECT 'wednesday', * FROM v_ends SETTINGS week_functions_starting_day = 'wednesday';
SELECT 'thursday', * FROM v_ends SETTINGS week_functions_starting_day = 'thursday';
SELECT 'friday', * FROM v_ends SETTINGS week_functions_starting_day = 'friday';
SELECT 'saturday', * FROM v_ends SETTINGS week_functions_starting_day = 'saturday';
SELECT 'sunday', * FROM v_ends SETTINGS week_functions_starting_day = 'sunday';

-- Outside the lookup table, where days are computed without it: the week and the 2-week interval of Wednesday
-- 0005-06-22, and the weeks from Wednesday 0005-06-01 to Saturday 0005-06-11.
CREATE VIEW v_outside AS SELECT
    toStartOfWeek(toDate32('0005-06-22')) AS week_start,
    toStartOfInterval(toDate32('0005-06-22'), INTERVAL 2 WEEK) AS week_2,
    dateDiff('week', toDate32('0005-06-01'), toDate32('0005-06-11')) AS dateDiff;

SELECT 'monday' AS starting_day, * FROM v_outside SETTINGS week_functions_starting_day = 'monday' FORMAT TSVWithNames;
SELECT 'tuesday', * FROM v_outside SETTINGS week_functions_starting_day = 'tuesday';
SELECT 'wednesday', * FROM v_outside SETTINGS week_functions_starting_day = 'wednesday';
SELECT 'thursday', * FROM v_outside SETTINGS week_functions_starting_day = 'thursday';
SELECT 'friday', * FROM v_outside SETTINGS week_functions_starting_day = 'friday';
SELECT 'saturday', * FROM v_outside SETTINGS week_functions_starting_day = 'saturday';
SELECT 'sunday', * FROM v_outside SETTINGS week_functions_starting_day = 'sunday';
SET enable_extended_results_for_datetime_functions = 0;

-- The last days of the lookup table, Sunday 2299-12-24 to Sunday 2299-12-31: the week number goes up on the start day,
-- and the two Sundays are one week apart for every start day. The day after the start of the last week is past the end
-- of the table, so it must not be computed through the table.
CREATE VIEW v_table_end AS SELECT
    arrayMap(n -> toRelativeWeekNum(toDate32('2299-12-24') + n) - toRelativeWeekNum(toDate32('2299-12-24')), range(8)) AS toRelativeWeekNum,
    dateDiff('week', toDate32('2299-12-24'), toDate32('2299-12-31')) AS dateDiff,
    dateDiff('week', toDateTime64('2299-12-24 12:00:00', 3, 'UTC'), toDateTime64('2299-12-31 12:00:00', 3, 'UTC')) AS dateDiff_datetime64;

SELECT 'monday' AS starting_day, * FROM v_table_end SETTINGS week_functions_starting_day = 'monday' FORMAT TSVWithNames;
SELECT 'tuesday', * FROM v_table_end SETTINGS week_functions_starting_day = 'tuesday';
SELECT 'wednesday', * FROM v_table_end SETTINGS week_functions_starting_day = 'wednesday';
SELECT 'thursday', * FROM v_table_end SETTINGS week_functions_starting_day = 'thursday';
SELECT 'friday', * FROM v_table_end SETTINGS week_functions_starting_day = 'friday';
SELECT 'saturday', * FROM v_table_end SETTINGS week_functions_starting_day = 'saturday';
SELECT 'sunday', * FROM v_table_end SETTINGS week_functions_starting_day = 'sunday';

-- The other input types, and `Nullable`, give the same results as `Date`. Each column is the number of days where one
-- of them differs.
SELECT
    countIf(toDayOfWeek(d32) != toDayOfWeek(d)) + countIf(toDayOfWeek(dt) != toDayOfWeek(d)) + countIf(toDayOfWeek(dt64) != toDayOfWeek(d)) AS toDayOfWeek,
    countIf(toStartOfWeek(d32) != toStartOfWeek(d)) + countIf(toStartOfWeek(dt) != toStartOfWeek(d)) + countIf(toStartOfWeek(dt64) != toStartOfWeek(d)) AS toStartOfWeek,
    countIf(toLastDayOfWeek(d32) != toLastDayOfWeek(d)) + countIf(toLastDayOfWeek(dt) != toLastDayOfWeek(d)) + countIf(toLastDayOfWeek(dt64) != toLastDayOfWeek(d)) AS toLastDayOfWeek,
    countIf(toWeek(d32) != toWeek(d)) + countIf(toWeek(dt) != toWeek(d)) + countIf(toWeek(dt64) != toWeek(d)) AS toWeek,
    countIf(toYearWeek(d32) != toYearWeek(d)) + countIf(toYearWeek(dt) != toYearWeek(d)) + countIf(toYearWeek(dt64) != toYearWeek(d)) AS toYearWeek,
    countIf(toDate(toStartOfInterval(d32, INTERVAL 2 WEEK)) != toStartOfInterval(d, INTERVAL 2 WEEK))
        + countIf(toDate(toStartOfInterval(dt, INTERVAL 2 WEEK)) != toStartOfInterval(d, INTERVAL 2 WEEK))
        + countIf(toDate(toStartOfInterval(dt64, INTERVAL 2 WEEK)) != toStartOfInterval(d, INTERVAL 2 WEEK)) AS toStartOfInterval,
    countIf(toDate(date_trunc('week', dt)) != date_trunc('week', d)) + countIf(toDate(date_trunc('week', dt64)) != date_trunc('week', d)) AS date_trunc,
    countIf(toRelativeWeekNum(d32) != toRelativeWeekNum(d)) + countIf(toRelativeWeekNum(dt) != toRelativeWeekNum(d)) + countIf(toRelativeWeekNum(dt64) != toRelativeWeekNum(d)) AS toRelativeWeekNum,
    countIf(dateDiff('week', toDate32('2016-12-27'), d32) != dateDiff('week', toDate('2016-12-27'), d))
        + countIf(dateDiff('week', toDateTime('2016-12-27 00:00:00', 'UTC'), dt) != dateDiff('week', toDate('2016-12-27'), d))
        + countIf(dateDiff('week', toDateTime64('2016-12-27 00:00:00', 3, 'UTC'), dt64) != dateDiff('week', toDate('2016-12-27'), d))
        + countIf(dateDiff('week', toDate('2016-12-27'), dt64) != dateDiff('week', toDate('2016-12-27'), d)) AS dateDiff,
    countIf(formatDateTime(d32, '%w') != formatDateTime(d, '%w')) + countIf(formatDateTime(dt, '%w') != formatDateTime(d, '%w')) + countIf(formatDateTime(dt64, '%w') != formatDateTime(d, '%w')) AS `%w`,
    countIf(fromUnixTimestamp(toUnixTimestamp(dt), '%w', 'UTC') != formatDateTime(d, '%w')) AS fromUnixTimestamp,
    countIf(dateName('week', d32) != dateName('week', d)) + countIf(dateName('week', dt) != dateName('week', d)) + countIf(dateName('week', dt64) != dateName('week', d)) AS dateName,
    countIf(toStartOfWeek(toNullable(d)) != toStartOfWeek(d)) + countIf(toDayOfWeek(toNullable(d)) != toDayOfWeek(d))
        + countIf(toRelativeWeekNum(toNullable(d)) != toRelativeWeekNum(d)) + countIf(toStartOfInterval(toNullable(d), INTERVAL 1 WEEK) != toStartOfInterval(d, INTERVAL 1 WEEK)) AS nullable
FROM t_days
SETTINGS week_functions_starting_day = 'saturday'
FORMAT TSVWithNames;

-- A time zone decides the local day: 20:00 on Friday 2026-01-02 in UTC is 01:45 on Saturday in Kathmandu, so with weeks
-- from Saturday it is in the week from 2026-01-03, whether the time zone is the one of the column or an argument.
SELECT
    toStartOfWeek(dt) AS toStartOfWeek,
    toDayOfWeek(dt) AS toDayOfWeek,
    toStartOfInterval(dt, INTERVAL 1 WEEK) AS toStartOfInterval,
    toRelativeWeekNum(dt) AS toRelativeWeekNum,
    formatDateTime(dt, '%w') AS `%w`
FROM (SELECT toDateTime('2026-01-02 20:00:00', 'UTC')::DateTime('Asia/Kathmandu') AS dt)
SETTINGS week_functions_starting_day = 'saturday'
FORMAT TSVWithNames;
SELECT
    toStartOfInterval(dt, INTERVAL 1 WEEK, 'Asia/Kathmandu') AS toStartOfInterval,
    date_trunc('week', dt, 'Asia/Kathmandu') AS date_trunc,
    toRelativeWeekNum(dt, 'Asia/Kathmandu') AS toRelativeWeekNum,
    dateDiff('week', toDateTime('2026-01-02 12:00:00', 'UTC'), dt, 'Asia/Kathmandu') AS dateDiff,
    formatDateTime(dt, '%w', 'Asia/Kathmandu') AS `%w`
FROM (SELECT toDateTime('2026-01-02 20:00:00', 'UTC') AS dt)
SETTINGS week_functions_starting_day = 'saturday'
FORMAT TSVWithNames;

-- `String` arguments are parsed before the week calculation. `toStartOfWeek` and `toLastDayOfWeek` don't take them.
SELECT toWeek('2026-01-03'), toYearWeek('2026-01-03'), toDayOfWeek('2026-01-03'), toWeek(toDate('2026-01-03')), toYearWeek(toDate('2026-01-03'))
SETTINGS week_functions_starting_day = 'saturday', week_functions_first_week_of_year = 'contains_january_1';

-- https://github.com/ClickHouse/ClickHouse/issues/45583: from Sunday to Monday is one week by default, and no week when
-- weeks start on Sunday. From Monday to Tuesday is no week in both cases.
SELECT dateDiff('week', toDate('2023-01-22'), toDate('2023-01-23')), dateDiff('week', toDate('2023-01-23'), toDate('2023-01-24'));
SELECT dateDiff('week', toDate('2023-01-22'), toDate('2023-01-23')), dateDiff('week', toDate('2023-01-23'), toDate('2023-01-24'))
SETTINGS week_functions_starting_day = 'sunday';

-- Every spelling of `dateDiff` with weeks follows the setting: from Friday to Saturday is one week.
SELECT
    dateDiff('weeks', toDate('2026-01-02'), toDate('2026-01-03')),
    dateDiff('wk', toDate('2026-01-02'), toDate('2026-01-03')),
    dateDiff('ww', toDate('2026-01-02'), toDate('2026-01-03')),
    date_diff('week', toDate('2026-01-02'), toDate('2026-01-03')),
    timestampDiff('week', toDate('2026-01-02'), toDate('2026-01-03')),
    DATEDIFF(WEEK, toDate('2026-01-02'), toDate('2026-01-03')),
    TIMESTAMPDIFF(WEEK, toDate('2026-01-02'), toDate('2026-01-03'))
SETTINGS week_functions_starting_day = 'saturday';

-- `age` counts whole weeks of 7 days and `timeDiff` counts seconds, so the setting doesn't change them: from Friday to
-- Saturday is one week for `dateDiff`, but not for `age`.
SELECT
    dateDiff('week', toDate('2026-01-02'), toDate('2026-01-03')) AS dateDiff,
    age('week', toDate('2026-01-02'), toDate('2026-01-03')) AS age_1_day,
    age('week', toDate('2026-01-02'), toDate('2026-01-09')) AS age_7_days,
    age('week', toDateTime64('2026-01-02 12:00:00', 3, 'UTC'), toDateTime64('2026-01-03 12:00:00', 3, 'UTC')) AS age_datetime64,
    timeDiff(toDate('2026-01-02'), toDate('2026-01-03')) AS timeDiff,
    timeDiff(toDateTime64('2026-01-02 00:00:00.000', 3, 'UTC'), toDateTime64('2026-01-02 00:00:01.500', 3, 'UTC')) AS timeDiff_datetime64
SETTINGS week_functions_starting_day = 'saturday'
FORMAT TSVWithNames;

-- The setting is sent with the query to the other servers.
SELECT toStartOfWeek(toDate('2026-01-02')), toDayOfWeek(toDate('2026-01-03')) FROM remote('127.0.0.{1,2}', system.one)
SETTINGS week_functions_starting_day = 'saturday';

SELECT '-- the week of the year';
-- 2024-01-01 is a Monday: the week from Saturday 2023-12-30 has 5 days in 2024, so `first_full_week` and
-- `four_or_more_days` differ. 2026-01-01 is a Thursday: the week from Saturday 2025-12-27 has 2 days in 2026, so
-- `contains_january_1` differs from the other two.
CREATE TABLE t_saturday (d Date) ENGINE = Memory;
INSERT INTO t_saturday VALUES ('2023-12-29'), ('2023-12-30'), ('2024-01-01'), ('2024-01-06'), ('2025-12-26'), ('2025-12-27'), ('2026-01-01'), ('2026-01-03');
CREATE VIEW v_saturday AS SELECT groupArray(toWeek(d)) AS toWeek, groupArray(toYearWeek(d)) AS toYearWeek FROM (SELECT d FROM t_saturday ORDER BY d);

SELECT 'dates', groupArray(d) FROM (SELECT d FROM t_saturday ORDER BY d);
SELECT 'saturday first_full_week 0-53', * FROM v_saturday
SETTINGS week_functions_starting_day = 'saturday', week_functions_first_week_of_year = 'first_full_week', week_functions_range = '0-53';
SELECT 'saturday first_full_week 1-53', * FROM v_saturday
SETTINGS week_functions_starting_day = 'saturday', week_functions_first_week_of_year = 'first_full_week', week_functions_range = '1-53';
SELECT 'saturday four_or_more_days 0-53', * FROM v_saturday
SETTINGS week_functions_starting_day = 'saturday', week_functions_first_week_of_year = 'four_or_more_days', week_functions_range = '0-53';
SELECT 'saturday four_or_more_days 1-53', * FROM v_saturday
SETTINGS week_functions_starting_day = 'saturday', week_functions_first_week_of_year = 'four_or_more_days', week_functions_range = '1-53';
SELECT 'saturday contains_january_1 0-53', * FROM v_saturday
SETTINGS week_functions_starting_day = 'saturday', week_functions_first_week_of_year = 'contains_january_1', week_functions_range = '0-53';
SELECT 'saturday contains_january_1 1-53', * FROM v_saturday
SETTINGS week_functions_starting_day = 'saturday', week_functions_first_week_of_year = 'contains_january_1', week_functions_range = '1-53';

-- `dateName('week')` defaults to the ISO week, not `mode` 0 like `toWeek`; so with only the start day set,
-- it is the week of `four_or_more_days 1-53` above.
SELECT 'dateName, saturday only', groupArray(dateName('week', d)) FROM (SELECT d FROM t_saturday ORDER BY d)
SETTINGS week_functions_starting_day = 'saturday';

-- By default, `dateName('week')` is the ISO week, for every input type.
SELECT 'dateName auto', countIf(dateName('week', d) != toString(toISOWeek(d))) + countIf(dateName('week', d32) != toString(toISOWeek(d)))
    + countIf(dateName('week', dt) != toString(toISOWeek(d))) + countIf(dateName('week', dt64) != toString(toISOWeek(d)))
FROM t_days;
-- Each setting replaces its part of the ISO week (`mode` 3: Monday, 1-53, `four_or_more_days`). Each row is the number
-- of days where `dateName('week')` differs from `toWeek` with the `mode` that has the same parts.
SELECT 'dateName sunday', countIf(dateName('week', d) != toString(toWeek(d, 6))) FROM t_days SETTINGS week_functions_starting_day = 'sunday';
SELECT 'dateName 0-53', countIf(dateName('week', d) != toString(toWeek(d, 1))) FROM t_days SETTINGS week_functions_range = '0-53';
SELECT 'dateName first_full_week', countIf(dateName('week', d) != toString(toWeek(d, 7))) FROM t_days SETTINGS week_functions_first_week_of_year = 'first_full_week';
SELECT 'dateName contains_january_1', countIf(dateName('week', d) != toString(toWeek(d, 9))) FROM t_days SETTINGS week_functions_first_week_of_year = 'contains_january_1';
-- With all three settings, `dateName('week')` and `toWeek` without `mode` are the same.
SELECT 'dateName saturday 0-53 first_full_week', countIf(dateName('week', d) != toString(toWeek(d))) FROM t_days
SETTINGS week_functions_starting_day = 'saturday', week_functions_range = '0-53', week_functions_first_week_of_year = 'first_full_week';

SELECT '-- primary key analysis';
-- `toDayOfWeek` is monotonic only inside a week that starts on its first day, and the index analysis only knows weeks
-- that start on Monday. With another start day, a filter on `toDayOfWeek` must return the same rows as without the
-- primary key.
CREATE TABLE t_week_key (d Date) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 3, index_granularity_bytes = 0;
-- Six weeks, from Monday 2025-12-01.
INSERT INTO t_week_key SELECT toDate('2025-12-01') + number FROM numbers(42);

SELECT 'saturday';
SET week_functions_starting_day = 'saturday';
SELECT count(), groupArray(d) FROM (SELECT d FROM t_week_key WHERE toDayOfWeek(d) = 1 ORDER BY d);
SELECT count(), groupArray(d) FROM (SELECT d FROM t_week_key WHERE toDayOfWeek(d) = 1 ORDER BY d) SETTINGS use_primary_key = 0;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d) <= 2;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d) <= 2 SETTINGS use_primary_key = 0;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d) >= 6;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d) >= 6 SETTINGS use_primary_key = 0;
-- An explicit Monday-first mode.
SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 1) = 5;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 1) = 5 SETTINGS use_primary_key = 0;

SELECT 'sunday';
SET week_functions_starting_day = 'sunday';
SELECT count(), groupArray(d) FROM (SELECT d FROM t_week_key WHERE toDayOfWeek(d) = 1 ORDER BY d);
SELECT count(), groupArray(d) FROM (SELECT d FROM t_week_key WHERE toDayOfWeek(d) = 1 ORDER BY d) SETTINGS use_primary_key = 0;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d) >= 7;
SELECT count() FROM t_week_key WHERE toDayOfWeek(d) >= 7 SETTINGS use_primary_key = 0;
SET week_functions_starting_day = 'auto';

-- How many of the 14 granules (3 days each) the primary key keeps, from `EXPLAIN indexes = 1`. By default it prunes.
-- With weeks from Saturday it keeps all of them, also for an explicit Monday-first mode: the monotonicity of the
-- function is decided by the setting, because it cannot see whether `mode` is given. Only the `PrimaryKey` section
-- is read: other indexes can be added by random settings.
SELECT 'granules auto', arrayFirst(x -> x LIKE 'Granules: %/%', arraySlice(lines, indexOf(lines, 'PrimaryKey')))
FROM (SELECT groupArray(trim(explain)) AS lines FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_week_key WHERE toDayOfWeek(d) = 1
    SETTINGS use_query_condition_cache = 0, enable_parallel_replicas = 0));
SELECT 'granules saturday', arrayFirst(x -> x LIKE 'Granules: %/%', arraySlice(lines, indexOf(lines, 'PrimaryKey')))
FROM (SELECT groupArray(trim(explain)) AS lines FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_week_key WHERE toDayOfWeek(d) = 1
    SETTINGS use_query_condition_cache = 0, enable_parallel_replicas = 0, week_functions_starting_day = 'saturday'));
SELECT 'granules auto, explicit Monday-first mode', arrayFirst(x -> x LIKE 'Granules: %/%', arraySlice(lines, indexOf(lines, 'PrimaryKey')))
FROM (SELECT groupArray(trim(explain)) AS lines FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 1) = 5
    SETTINGS use_query_condition_cache = 0, enable_parallel_replicas = 0));
SELECT 'granules saturday, explicit Monday-first mode', arrayFirst(x -> x LIKE 'Granules: %/%', arraySlice(lines, indexOf(lines, 'PrimaryKey')))
FROM (SELECT groupArray(trim(explain)) AS lines FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_week_key WHERE toDayOfWeek(d, 1) = 5
    SETTINGS use_query_condition_cache = 0, enable_parallel_replicas = 0, week_functions_starting_day = 'saturday'));

SELECT '-- query condition cache';
-- The query condition cache remembers which granules match a filter. The week functions compute different values under
-- different settings, so a cache entry recorded under one setting must not be used under another. Each first query
-- finds no match, and the cache would make the second one return 0 too.
SET use_query_condition_cache = 1;
CREATE TABLE t_week_cache (id UInt64, d Date) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;
-- 2025-12-18 to 2026-01-14.
INSERT INTO t_week_cache SELECT number, toDate('2025-12-18') + number FROM numbers(28);

-- 2025-12-27 is a Saturday: no week starts on it by default, and one week starts on it with 'saturday'.
SELECT count() FROM t_week_cache WHERE toStartOfWeek(d) = toDate('2025-12-27');
SELECT count() FROM t_week_cache WHERE toStartOfWeek(d) = toDate('2025-12-27') SETTINGS week_functions_starting_day = 'saturday';
SELECT count() FROM t_week_cache WHERE toStartOfWeek(d) = toDate('2025-12-27');

-- 2025-12-28 is a Sunday: the other way around.
SELECT count() FROM t_week_cache WHERE toStartOfWeek(d) = toDate('2025-12-28') SETTINGS week_functions_starting_day = 'saturday';
SELECT count() FROM t_week_cache WHERE toStartOfWeek(d) = toDate('2025-12-28');

-- By default (weeks from Sunday, 0-53, `first_full_week`), 2026-01-01 to 2026-01-03 are in week 0. With 1-53, or with
-- `contains_january_1`, there is no week 0.
SELECT count() FROM t_week_cache WHERE toWeek(d) = 0 SETTINGS week_functions_range = '1-53';
SELECT count() FROM t_week_cache WHERE toWeek(d) = 0;
SELECT count() FROM t_week_cache WHERE toWeek(d) < 1 SETTINGS week_functions_first_week_of_year = 'contains_january_1';
SELECT count() FROM t_week_cache WHERE toWeek(d) < 1;

SELECT '-- stored expressions';
-- Stored expressions that call week functions without `mode` depend on `week_functions_starting_day`. This records
-- the behaviour that the documentation of the setting warns about.
-- 2026-01-02 is a Friday: its week starts on Sunday 2025-12-28 by default, or on Saturday 2025-12-27.

-- A wrong result of the index analysis below would be remembered by the query condition cache and change later results.
SET use_query_condition_cache = 0;

SELECT 'partition key';
-- The partition key is computed with the server's settings, not with the setting of the session.
SET week_functions_starting_day = 'saturday';
CREATE TABLE t_week_partition (d Date) ENGINE = MergeTree PARTITION BY toStartOfWeek(d) ORDER BY d;
INSERT INTO t_week_partition VALUES ('2026-01-02');
SELECT partition FROM system.parts WHERE database = currentDatabase() AND table = 't_week_partition' AND active;
DETACH TABLE t_week_partition;
ATTACH TABLE t_week_partition;
INSERT INTO t_week_partition VALUES ('2026-01-02');
SELECT partition FROM system.parts WHERE database = currentDatabase() AND table = 't_week_partition' AND active;

-- A query with a different setting computes other values. Known issue: partition pruning compares them with the stored
-- ones and skips the matching partition, so the first query returns 0 instead of 2. The index analysis matches the
-- expressions by name (https://github.com/ClickHouse/ClickHouse/issues/121844).
SELECT count() FROM t_week_partition WHERE toStartOfWeek(d) = toDate('2025-12-27');
SELECT countIf(toStartOfWeek(d) = toDate('2025-12-27')) FROM t_week_partition;
SET week_functions_starting_day = 'auto';

SELECT 'sorting key';
-- The same with the primary key: the first query returns 1 instead of 7.
CREATE TABLE t_week_sorting (d Date) ENGINE = MergeTree ORDER BY toStartOfWeek(d) SETTINGS index_granularity = 1;
INSERT INTO t_week_sorting SELECT toDate('2025-12-18') + number FROM numbers(28);
SELECT count() FROM t_week_sorting WHERE toStartOfWeek(d) = toDate('2025-12-27') SETTINGS week_functions_starting_day = 'saturday';
SELECT countIf(toStartOfWeek(d) = toDate('2025-12-27')) FROM t_week_sorting SETTINGS week_functions_starting_day = 'saturday';

SELECT 'DEFAULT and MATERIALIZED columns';
-- They are computed with the setting of each `INSERT`.
CREATE TABLE t_week_default (d Date, w_default Date DEFAULT toStartOfWeek(d), w_materialized Date MATERIALIZED toStartOfWeek(d))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_week_default (d) VALUES ('2026-01-02');
INSERT INTO t_week_default (d) SETTINGS week_functions_starting_day = 'saturday' VALUES ('2026-01-02');
SELECT d, w_default, w_materialized FROM t_week_default ORDER BY w_default;

DROP VIEW v_explicit;
DROP VIEW v_agreement;
DROP VIEW v_values;
DROP VIEW v_ends;
DROP VIEW v_outside;
DROP VIEW v_table_end;
DROP VIEW v_saturday;
DROP TABLE t_examples;
DROP TABLE t_new_year;
DROP TABLE t_days;
DROP TABLE t_expected;
DROP TABLE t_saturday;
DROP TABLE t_week_key;
DROP TABLE t_week_cache;
DROP TABLE t_week_partition;
DROP TABLE t_week_sorting;
DROP TABLE t_week_default;
