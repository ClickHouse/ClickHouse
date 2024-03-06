SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.000999', 6), toIntervalMillisecond(1));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.000500', 6), toIntervalMillisecond(1));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.000499', 6), toIntervalMillisecond(1));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.000999', 6), toIntervalMillisecond(10));
select toStartOfInterval(toDateTime64('2023-10-09 00:01:34', 9), toIntervalMicrosecond(100000000));
select toStartOfInterval(toDateTime64('2023-10-09 00:01:34', 9), toIntervalMillisecond(100000));
select toStartOfInterval(toDateTime64('2023-10-09 00:01:34', 9), toIntervalSecond(100));