-- toStartOfInterval with an explicit origin must floor to the interval grid anchored at the origin
-- when the input scale is finer than the interval unit, consistently with the two-argument overload.

-- millisecond interval with microsecond input, epoch origin: must match the two-argument overload
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.129999', 6, 'UTC'), toIntervalMillisecond(10), toDateTime64('1970-01-01 00:00:00', 6, 'UTC'));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.000999', 6, 'UTC'), toIntervalMillisecond(1), toDateTime64('1970-01-01 00:00:00', 6, 'UTC'));
-- must NOT cross the second boundary
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.999999', 6, 'UTC'), toIntervalMillisecond(1), toDateTime64('1970-01-01 00:00:00', 6, 'UTC'));

-- non-epoch origin: the grid is anchored at the origin, not at the epoch
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.129999', 6, 'UTC'), toIntervalMillisecond(10), toDateTime64('2023-10-09 10:11:12.005000', 6, 'UTC'));

-- microsecond interval with nanosecond input
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.123459500', 9, 'UTC'), toIntervalMicrosecond(10), toDateTime64('1970-01-01 00:00:00', 9, 'UTC'));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.123459500', 9, 'UTC'), toIntervalMicrosecond(10), toDateTime64('2023-10-09 10:11:12.123453000', 9, 'UTC'));

-- input scale coarser than the interval unit: microsecond input, nanosecond interval
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.123456', 6, 'UTC'), toIntervalNanosecond(10), toDateTime64('1970-01-01 00:00:00', 6, 'UTC'));

-- origin before the epoch
SELECT toStartOfInterval(toDateTime64('1970-01-01 00:00:00.007999', 6, 'UTC'), toIntervalMillisecond(10), toDateTime64('1969-12-31 23:59:59.995000', 6, 'UTC'));

-- a negative interval start with a sub-unit part must be floored to the result scale, not rounded towards zero,
-- otherwise the result is after the time argument
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:59:59.998600', 6, 'UTC'), toIntervalMillisecond(1), toDateTime64('1969-12-31 23:59:59.998500', 6, 'UTC'));
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:59:59.999600', 6, 'UTC'), toIntervalMillisecond(1), toDateTime64('1969-12-31 23:59:59.999000', 6, 'UTC'));

-- the three-argument overload with the epoch origin agrees with the two-argument overload
SELECT toStartOfInterval(t, toIntervalMillisecond(10), toDateTime64('1970-01-01 00:00:00', 6, 'UTC')) = toStartOfInterval(t, toIntervalMillisecond(10))
FROM (SELECT toDateTime64('2023-10-09 10:11:12.129999', 6, 'UTC') AS t);
