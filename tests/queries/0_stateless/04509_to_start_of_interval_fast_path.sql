-- Tests for the arithmetic fast path of toStartOfInterval for SECOND / MINUTE / HOUR intervals.
-- The fast path applies to time zones whose offset is minute-aligned (respectively hour-aligned)
-- since the epoch; other time zones and interval kinds take the generic path.

-- Equivalence with the dedicated functions (which use the same rounding via a compile-time constant).
SELECT 'equivalence with dedicated functions';
SELECT countIf(toStartOfInterval(t, INTERVAL 1 MINUTE) != toStartOfMinute(t))
     + countIf(toStartOfInterval(t, INTERVAL 5 MINUTE) != toStartOfFiveMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 15 MINUTE) != toStartOfFifteenMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 1 HOUR) != toStartOfHour(t))
FROM (SELECT toDateTime(1500000000 + number * 3607, 'UTC') AS t FROM numbers(100000));
SELECT countIf(toStartOfInterval(t, INTERVAL 1 MINUTE) != toStartOfMinute(t))
     + countIf(toStartOfInterval(t, INTERVAL 5 MINUTE) != toStartOfFiveMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 15 MINUTE) != toStartOfFifteenMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 1 HOUR) != toStartOfHour(t))
FROM (SELECT toDateTime(1500000000 + number * 3607, 'Asia/Kolkata') AS t FROM numbers(100000));
SELECT countIf(toStartOfInterval(t, INTERVAL 1 MINUTE) != toStartOfMinute(t))
     + countIf(toStartOfInterval(t, INTERVAL 5 MINUTE) != toStartOfFiveMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 15 MINUTE) != toStartOfFifteenMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 1 HOUR) != toStartOfHour(t))
FROM (SELECT toDateTime(1500000000 + number * 3607, 'Australia/Lord_Howe') AS t FROM numbers(100000));
SELECT countIf(toStartOfInterval(t, INTERVAL 1 MINUTE) != toStartOfMinute(t))
     + countIf(toStartOfInterval(t, INTERVAL 5 MINUTE) != toStartOfFiveMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 15 MINUTE) != toStartOfFifteenMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 1 HOUR) != toStartOfHour(t))
FROM (SELECT toDateTime(1500000000 + number * 3607, 'Africa/Monrovia') AS t FROM numbers(100000));

-- Modular arithmetic identity in UTC, including intervals that do not divide an hour or a minute.
SELECT 'modular arithmetic in UTC';
SELECT countIf(toUInt32(toStartOfInterval(t, INTERVAL 90 SECOND)) != toUInt32(t) - toUInt32(t) % 90)
     + countIf(toUInt32(toStartOfInterval(t, INTERVAL 7 SECOND)) != toUInt32(t) - toUInt32(t) % 7)
     + countIf(toUInt32(toStartOfInterval(t, INTERVAL 7 MINUTE)) != toUInt32(t) - toUInt32(t) % 420)
FROM (SELECT toDateTime(1500000000 + number * 61, 'UTC') AS t FROM numbers(100000));

-- The same equivalences for DateTime64 arguments.
SELECT 'DateTime64 equivalence';
SELECT countIf(toStartOfInterval(t, INTERVAL 5 MINUTE) != toStartOfFiveMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 1 HOUR) != toStartOfHour(t))
FROM (SELECT toDateTime64(1500000000 + number * 3.607, 3, 'UTC') AS t FROM numbers(100000));
SELECT countIf(toStartOfInterval(t, INTERVAL 5 MINUTE) != toStartOfFiveMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 1 HOUR) != toStartOfHour(t))
FROM (SELECT toDateTime64(1500000000 + number * 3.607, 3, 'Australia/Lord_Howe') AS t FROM numbers(100000));

-- Exact values: interval of one second is the identity.
SELECT 'identity';
SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12', 'UTC'), INTERVAL 1 SECOND);
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987', 3, 'UTC'), INTERVAL 1 SECOND);

-- Exact values around a DST transition in a zone with a 30-minute shift (2024-04-07 02:00 -> 01:30).
-- Minute intervals use the fast path there, hour intervals must take the generic path.
SELECT 'Lord Howe DST transition';
SELECT toStartOfInterval(toDateTime('2024-04-07 01:45:10', 'Australia/Lord_Howe'), INTERVAL 15 MINUTE);
SELECT toStartOfInterval(toDateTime('2024-04-07 01:45:10', 'Australia/Lord_Howe'), INTERVAL 1 HOUR);
SELECT toStartOfInterval(toDateTime('2024-04-07 03:10:00', 'Australia/Lord_Howe'), INTERVAL 2 HOUR);

-- Values outside the lookup table (before 1900, after 2299): the offset is extrapolated there and can have
-- a sub-minute component (`Asia/Kolkata` is +5:53:28 before 1906), so they must keep the generic path.
SELECT 'out of LUT range';
SELECT countIf(toStartOfInterval(t, INTERVAL 1 MINUTE) != toStartOfMinute(t))
     + countIf(toStartOfInterval(t, INTERVAL 5 MINUTE) != toStartOfFiveMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 15 MINUTE) != toStartOfFifteenMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 1 HOUR) != toStartOfHour(t))
FROM (SELECT toDateTime64(arrayJoin([-2209000000., -3786749363., 16725303245., 10413800000.]) + number * 97.13, 3, 'Asia/Kolkata') AS t FROM numbers(20000));
SELECT countIf(toStartOfInterval(t, INTERVAL 1 MINUTE) != toStartOfMinute(t))
     + countIf(toStartOfInterval(t, INTERVAL 5 MINUTE) != toStartOfFiveMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 15 MINUTE) != toStartOfFifteenMinutes(t))
     + countIf(toStartOfInterval(t, INTERVAL 1 HOUR) != toStartOfHour(t))
FROM (SELECT toDateTime64(arrayJoin([-2209000000., -3786749363., 16725303245., 10413800000.]) + number * 97.13, 3, 'Europe/Moscow') AS t FROM numbers(20000));

-- Pre-epoch DateTime64 values.
SELECT 'pre-epoch';
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:59:58.123', 3, 'UTC'), INTERVAL 7 SECOND);
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:59:58.123', 3, 'UTC'), INTERVAL 5 MINUTE);
SELECT toStartOfInterval(toDateTime64('1900-01-02 03:04:05.678', 3, 'Asia/Kolkata'), INTERVAL 15 MINUTE);

-- Extreme interval counts: the divisor saturates instead of overflowing.
SELECT 'extreme interval counts';
SELECT toStartOfInterval(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 4611686018427387904 MINUTE);
SELECT toStartOfInterval(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 9223372036854775807 SECOND);
SELECT toStartOfInterval(toDateTime('2020-01-01 00:00:00', 'UTC'), INTERVAL 200000000000 MINUTE);
