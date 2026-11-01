-- The parsed value of a row must not depend on the text of the neighbouring row of the same column.
-- A value without a year gets the current year substituted, so the year is asserted relative to
-- today() and the remaining fields absolutely; that keeps the reference stable across calendar years.

-- A valid '1-Jan' must be accepted even though the next row starts with a space.
SELECT s, isNull(v) AS rejected,
          toYear(v) = toYear(today()) AS year_is_current,
          toMonth(v) AS m, toDayOfMonth(v) AS d,
          toHour(v) AS h, toMinute(v) AS mi, toSecond(v) AS sec
FROM (SELECT s, parseDateTimeBestEffortOrNull(s) AS v
      FROM (SELECT arrayJoin(['1-Jan', ' 12:00:00']) AS s));

-- addMonths over a String does not reject a partially parsed value, so on this surface the
-- neighbouring row leaks into the result instead of the value being rejected.
SELECT s, toYear(t) = toYear(today()) AS year_is_current,
          toMonth(t) AS m, toDayOfMonth(t) AS d, toHour(t) AS h
FROM (SELECT s, addMonths(s, 1) AS t
      FROM (SELECT arrayJoin(['1-Jan', ' 2020-01-02 03:04:05']) AS s));

-- Positive controls: a canonical value, a neighbour that cannot extend the value, and an
-- alphabetical month that carries its own year, all of which were already correct.
SELECT parseDateTimeBestEffortOrNull('2020-01-02 03:04:05');

SELECT s, toMonth(v) AS m, toDayOfMonth(v) AS d, toHour(v) AS h
FROM (SELECT s, parseDateTimeBestEffortOrNull(s) AS v
      FROM (SELECT arrayJoin(['1-Jan', '2020-01-02 03:04:05']) AS s));

SELECT parseDateTimeBestEffortOrNull('1-Jan-2020');

-- The last value of a column has no neighbour, so this is the arm the memory sanitizer observes.
SELECT parseDateTimeBestEffortOrNull(materialize(toFixedString('1-Jan', 5))) IS NOT NULL;
