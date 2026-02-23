SELECT
    number + 1 AS n,
    CAST(toIntervalHour(number + 1) AS IntervalMinute) AS minutes
FROM numbers(5);

SELECT
    number AS n,
    CAST(toIntervalSecond(number * 60) AS IntervalMinute) AS minutes
FROM numbers(5);

SELECT
    number * 60 AS n,
    CAST(toIntervalMinute(number * 60) AS IntervalHour) AS hours
FROM numbers(5);

SELECT
    number + 1 AS n,
    CAST(toIntervalDay(number + 1) AS IntervalHour) AS hours
FROM numbers(3);

SELECT CAST(toIntervalHour(2) AS IntervalMinute);
SELECT CAST(toIntervalMinute(120) AS IntervalHour);

SELECT CAST(toIntervalSecond(42) AS IntervalSecond);
