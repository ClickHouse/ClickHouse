SELECT '-----backwards-compatible function calls-----' AS COMMENT;
SELECT 'toWeek' AS fn, toDate('2016-12-27') AS date,
    toWeek(date) AS week0,
    toWeek(date, 1) AS week1,
    toWeek(date, 9) AS week9;
SELECT 'toYearWeek' AS fn, toDate('2016-12-27') AS date,
    toYearWeek(date) AS yearWeek0,
    toYearWeek(date, 1) AS yearWeek1,
    toYearWeek(date, 9) AS yearWeek9;
SELECT 'toDayOfWeek' AS fn, toDate('2016-12-27') AS date,
    toDayOfWeek(date) AS `day`,
    toDayOfWeek(date, 0) AS day0,
    toDayOfWeek(date, 1) AS day1,
    toDayOfWeek(date, 2) AS day2,
    toDayOfWeek(date, 3) AS day3;
SELECT 'toStartOfWeek' AS fn, toDate('2016-12-27') AS date,
    toStartOfWeek(date) AS weekStart0,
    toStartOfWeek(date, 1) AS weekStart1,
    toStartOfWeek(date, 9) AS weekStart9;
SELECT 'toLastDayOfWeek' AS fn, toDate('2016-12-27') AS date,
    toLastDayOfWeek(date) AS weekEnd0,
    toLastDayOfWeek(date, 1) AS weekEnd1,
    toLastDayOfWeek(date, 9) AS weekEnd9;
SELECT 'toStartOfInterval' AS fn, toDate('2016-12-27') AS date,
    toStartOfInterval(date, INTERVAL 1 week) AS weekStartInterval0;
SELECT 'toRelativeWeekNum' AS fn, toDate('2016-12-27') AS date,
    NOW() AS date2,
    toRelativeWeekNum(date) AS relWeek0,
    toRelativeWeekNum(date - INTERVAL 1 week) AS relWeek1,
    toRelativeWeekNum(toStartOfWeek(date2)) AS relWeekNow0,
    toRelativeWeekNum(toStartOfWeek(date2) - INTERVAL 1 week) AS relWeekNow1;


-- Taken from https://github.com/ClickHouse/ClickHouse/issues/45583:
-- returns 0
SELECT 'dateDiff' AS fn,
    dateDiff(
        'week',
        toDateTime('2023-01-23 00:00:00', 'UTC'),
        -- Monday
        toDateTime('2023-01-24 00:00:00', 'UTC')
    ) AS dateDiff0,
    -- returns 1
    dateDiff(
        'week',
        toDateTime('2023-01-22 00:00:00', 'UTC'),
        toDateTime('2023-01-23 00:00:00', 'UTC') -- Monday
    ) AS dateDiff1;

-- Joda Syntax support
SELECT 'formatDateTimeInJodaSyntax' AS fn, toDate('2016-12-27') AS date,
    formatDateTimeInJodaSyntax(date, 'yyyy-MM-dd HH:mm:ss') AS syntax,
    formatDateTimeInJodaSyntax(date, 'x') AS weekYear,
    formatDateTimeInJodaSyntax(date, 'w') AS yearWeek,
    formatDateTimeInJodaSyntax(date, 'e') AS weekDayNum,
    formatDateTimeInJodaSyntax(date, 'E') AS weekDayText;
SELECT 'parseDateTimeInJodaSyntax' as fn,
    parseDateTimeInJodaSyntax('2016 52 2', 'xxxx w e') as parsed0;
SELECT 'parseDateTimeInJodaSyntax' as fn,
    parseDateTimeInJodaSyntax('2016 52 Tue', 'xxxx w E') as parsed0;

-- TODO: enable_extended_results_for_datetime_functions setting
-- TODO: timezone?
-- TODO: formatDateTimeInJodaSyntax support for week formatting
-- TODO: wrong link in parseDateTimeInJodaSyntax docs

-- SELECT '-----new functionality-----' AS COMMENT;