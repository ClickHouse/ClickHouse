-- `toStartOfInterval` with an origin and a `MONTH`, `QUARTER` or `YEAR` interval keeps the time of day of the origin.
-- An argument on the same day of month as a bucket start, but earlier in the day than the origin, belongs to the previous bucket.

SET session_timezone = 'UTC';

SELECT 'DateTime';
SELECT toStartOfInterval(toDateTime('2023-10-08 09:08:06'), INTERVAL 1 MONTH, toDateTime('2023-09-08 09:08:07'));
SELECT toStartOfInterval(toDateTime('2023-10-08 09:08:07'), INTERVAL 1 MONTH, toDateTime('2023-09-08 09:08:07'));
SELECT toStartOfInterval(toDateTime('2023-10-08 09:08:08'), INTERVAL 1 MONTH, toDateTime('2023-09-08 09:08:07'));
SELECT toStartOfInterval(toDateTime('2023-11-08 09:08:06'), INTERVAL 2 MONTH, toDateTime('2023-09-08 09:08:07'));
SELECT toStartOfInterval(toDateTime('2023-12-08 09:08:06'), INTERVAL 1 QUARTER, toDateTime('2023-09-08 09:08:07'));
SELECT toStartOfInterval(toDateTime('2024-09-08 09:08:06'), INTERVAL 1 YEAR, toDateTime('2023-09-08 09:08:07'));
SELECT toStartOfInterval(toDateTime('2024-09-08 09:08:07'), INTERVAL 1 YEAR, toDateTime('2023-09-08 09:08:07'));

SELECT 'DateTime64';
SELECT toStartOfInterval(toDateTime64('2023-10-08 09:08:06.123', 3), INTERVAL 1 MONTH, toDateTime64('2023-09-08 09:08:07.123', 3));
SELECT toStartOfInterval(toDateTime64('2023-10-08 09:08:07.122', 3), INTERVAL 1 MONTH, toDateTime64('2023-09-08 09:08:07.123', 3));
SELECT toStartOfInterval(toDateTime64('2023-10-08 09:08:07.123', 3), INTERVAL 1 MONTH, toDateTime64('2023-09-08 09:08:07.123', 3));
SELECT toStartOfInterval(toDateTime64('2023-12-08 09:08:07.122', 3), INTERVAL 1 QUARTER, toDateTime64('2023-09-08 09:08:07.123', 3));
SELECT toStartOfInterval(toDateTime64('2024-09-08 09:08:07.122', 3), INTERVAL 1 YEAR, toDateTime64('2023-09-08 09:08:07.123', 3));

SELECT 'Date';
SELECT toStartOfInterval(toDate('2023-10-08'), INTERVAL 1 MONTH, toDate('2023-09-08'));
SELECT toStartOfInterval(toDate('2023-10-07'), INTERVAL 1 MONTH, toDate('2023-09-08'));

-- The bucket start is never after the argument.
SELECT 'Invariant';
WITH
    toDateTime64('2023-01-31 12:34:56.789', 3) AS origin,
    origin + toIntervalMillisecond(number * 7654321) AS t,
    toStartOfInterval(t, INTERVAL 1 MONTH, origin) AS m,
    toStartOfInterval(t, INTERVAL 1 QUARTER, origin) AS q,
    toStartOfInterval(t, INTERVAL 1 YEAR, origin) AS y
SELECT countIf(m > t), countIf(q > t), countIf(y > t)
FROM numbers(20000);

WITH
    toDateTime64('2021-03-28 02:30:00.500', 3, 'Europe/Amsterdam') AS origin,
    origin + toIntervalMillisecond(number * 7654321) AS t,
    toStartOfInterval(t, INTERVAL 1 MONTH, origin) AS m,
    toStartOfInterval(t, INTERVAL 1 QUARTER, origin) AS q,
    toStartOfInterval(t, INTERVAL 1 YEAR, origin) AS y
SELECT countIf(m > t), countIf(q > t), countIf(y > t)
FROM numbers(20000);

-- A bucket start clipped by `addMonths` to the end of a shorter month is rounded to itself.
SELECT 'Clipped';
SELECT toStartOfInterval(toDateTime('2023-02-28 12:00:00'), INTERVAL 1 MONTH, toDateTime('2023-01-31 12:00:00'));
SELECT toStartOfInterval(toDateTime('2023-02-28 11:59:59'), INTERVAL 1 MONTH, toDateTime('2023-01-31 12:00:00'));
SELECT toStartOfInterval(toDateTime('2023-04-30 12:00:00'), INTERVAL 1 QUARTER, toDateTime('2023-01-31 12:00:00'));
SELECT toStartOfInterval(toDateTime('2025-02-28 12:00:00'), INTERVAL 1 YEAR, toDateTime('2024-02-29 12:00:00'));
SELECT toStartOfInterval(toDate('2023-02-28'), INTERVAL 1 MONTH, toDate('2023-01-31'));

-- The same with a fractional origin before the epoch.
WITH
    toDateTime64('1969-01-29 23:59:59.500', 3) AS origin,
    toDateTime64('1969-02-28 23:59:59.500', 3) AS boundary
SELECT
    toStartOfInterval(boundary, INTERVAL 1 MONTH, origin),
    toStartOfInterval(boundary - toIntervalMillisecond(1), INTERVAL 1 MONTH, origin),
    toStartOfInterval(toDateTime64('1969-01-31 23:59:59.500', 3), INTERVAL 1 MONTH, toDateTime64('1968-12-31 23:59:59.500', 3));

-- Every bucket boundary is rounded to itself, and the value just before it is not.
WITH
    toDateTime64('1969-01-31 23:59:59.500', 3) AS origin,
    toDateTime64(toStartOfInterval(origin + toIntervalDay(number * 13), INTERVAL 1 MONTH, origin), 3) AS boundary
SELECT
    countIf(toStartOfInterval(boundary, INTERVAL 1 MONTH, origin) != boundary),
    countIf(boundary > origin AND toStartOfInterval(boundary - toIntervalMillisecond(1), INTERVAL 1 MONTH, origin) >= boundary)
FROM numbers(200);
