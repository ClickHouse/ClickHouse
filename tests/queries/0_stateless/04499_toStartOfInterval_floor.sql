-- toStartOfInterval must floor (truncate), not round, when input scale exceeds interval unit
-- millisecond interval with microsecond input
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.000999', 6, 'UTC'), toIntervalMillisecond(1));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.000500', 6, 'UTC'), toIntervalMillisecond(1));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.123999', 6, 'UTC'), toIntervalMillisecond(1));
-- must NOT cross second boundary
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.999999', 6, 'UTC'), toIntervalMillisecond(1));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.129999', 6, 'UTC'), toIntervalMillisecond(10));
-- microsecond interval with nanosecond input
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.123456789', 9, 'UTC'), INTERVAL 1 MICROSECOND);
-- must NOT cross second boundary
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.999999999', 9, 'UTC'), INTERVAL 1 MICROSECOND);
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.123459500', 9, 'UTC'), INTERVAL 10 MICROSECOND);
