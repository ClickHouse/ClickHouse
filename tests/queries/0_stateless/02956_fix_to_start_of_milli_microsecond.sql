SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.000999', 6), toIntervalMillisecond(1));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.000500', 6), toIntervalMillisecond(1));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.000499', 6), toIntervalMillisecond(1));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.000999', 6), toIntervalMillisecond(10));