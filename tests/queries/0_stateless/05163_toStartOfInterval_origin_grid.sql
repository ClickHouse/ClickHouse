-- With an origin, the buckets of `toStartOfInterval` are `origin + k * interval`. The fixed-length units used
-- to round the difference from the origin with the calendar helpers of `DateLUTImpl`, which align a time point
-- to its local midnight - but the difference is a duration, so the buckets came out shifted by the UTC offset
-- and re-anchored on every local day.

SELECT 'Time zones with a UTC offset that is not a whole number of hours';
SELECT toStartOfInterval(toDateTime('2020-06-15 13:01:05', 'Asia/Kolkata'), INTERVAL 1 HOUR, toDateTime('2020-06-15 12:00:00', 'Asia/Kolkata'));
SELECT toStartOfInterval(toDateTime('2020-06-15 13:00:10', 'Asia/Kolkata'), INTERVAL 7 SECOND, toDateTime('2020-06-15 12:00:00', 'Asia/Kolkata'));
SELECT toStartOfInterval(toDateTime('2020-06-15 13:00:10', 'Asia/Kathmandu'), INTERVAL 25 MINUTE, toDateTime('2020-06-15 12:00:00', 'Asia/Kathmandu'));

-- Before 1906 `Asia/Kolkata` is +5:53:28, so the offset has a sub-minute component as well.
SELECT toStartOfInterval(toDateTime64('1902-06-15 13:01:05', 0, 'Asia/Kolkata'), INTERVAL 1 HOUR, toDateTime64('1902-06-15 12:00:00', 0, 'Asia/Kolkata'));
SELECT toStartOfInterval(toDateTime64('1902-06-15 12:00:41', 0, 'Asia/Kolkata'), INTERVAL 20 SECOND, toDateTime64('1902-06-15 12:00:00', 0, 'Asia/Kolkata'));

SELECT 'The grid does not restart on every local day';
SELECT toStartOfInterval(toDateTime('2024-03-10 04:00:00', 'UTC'), INTERVAL 5 HOUR, toDateTime('2024-03-01 00:00:00', 'UTC'));
SELECT toStartOfInterval(toDateTime('2024-03-10 04:00:00', 'Europe/Amsterdam'), INTERVAL 5 HOUR, toDateTime('2024-03-01 00:00:00', 'Europe/Amsterdam'));
SELECT toStartOfInterval(toDateTime('2024-03-10 04:00:00', 'UTC'), INTERVAL 7 DAY, toDateTime('2024-03-01 00:00:00', 'UTC'));

SELECT 'The distance from the origin is a whole number of intervals, whatever the time zone';
SELECT
    toStartOfInterval(toDateTime('2023-10-09 10:11:12', 'UTC'), INTERVAL 1 DAY, toDateTime('2023-10-08 09:08:07', 'UTC')) - toDateTime('2023-10-08 09:08:07', 'UTC'),
    toStartOfInterval(toDateTime('2023-10-09 10:11:12', 'Europe/Amsterdam'), INTERVAL 1 DAY, toDateTime('2023-10-08 09:08:07', 'Europe/Amsterdam')) - toDateTime('2023-10-08 09:08:07', 'Europe/Amsterdam'),
    toStartOfInterval(toDateTime('2023-10-09 10:11:12', 'Asia/Kolkata'), INTERVAL 1 DAY, toDateTime('2023-10-08 09:08:07', 'Asia/Kolkata')) - toDateTime('2023-10-08 09:08:07', 'Asia/Kolkata'),
    toStartOfInterval(toDateTime('2023-10-09 10:11:12', 'Asia/Kathmandu'), INTERVAL 1 DAY, toDateTime('2023-10-08 09:08:07', 'Asia/Kathmandu')) - toDateTime('2023-10-08 09:08:07', 'Asia/Kathmandu'),
    toStartOfInterval(toDateTime('2023-10-09 10:11:12', 'America/St_Johns'), INTERVAL 1 DAY, toDateTime('2023-10-08 09:08:07', 'America/St_Johns')) - toDateTime('2023-10-08 09:08:07', 'America/St_Johns');

SELECT 'A summer time transition inside the interval does not move the grid';
SELECT toStartOfInterval(toDateTime('2024-03-31 12:00:00', 'Europe/Amsterdam'), INTERVAL 6 HOUR, toDateTime('2024-03-30 00:00:00', 'Europe/Amsterdam'));

SELECT 'Sub-second scales';
SELECT toStartOfInterval(toDateTime64('2020-06-15 13:01:05.500', 3, 'Asia/Kolkata'), INTERVAL 1 HOUR, toDateTime64('2020-06-15 12:00:00.000', 3, 'Asia/Kolkata'));
SELECT toStartOfInterval(toDateTime64('2020-06-15 13:01:05.500', 9, 'Asia/Kolkata'), INTERVAL 30 MINUTE, toDateTime64('2020-06-15 12:00:00.000', 9, 'Asia/Kolkata'));

SELECT 'Date and Date32 arguments';
SELECT toStartOfInterval(toDate('2026-07-27'), INTERVAL 10 DAY, toDate('2026-01-05'));
SELECT toStartOfInterval(toDate32('2026-07-27'), INTERVAL 10 DAY, toDate32('2026-01-05'));
