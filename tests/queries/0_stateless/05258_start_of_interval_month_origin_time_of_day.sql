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
