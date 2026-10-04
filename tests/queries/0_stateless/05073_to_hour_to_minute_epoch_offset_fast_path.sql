-- Tests the arithmetic fast paths of toHour and toMinute. toHour takes one in time zones whose UTC offset
-- does not change from the epoch onwards; toMinute takes one in time zones that keep a constant minute-of-hour
-- (a whole number of minutes, changing only by whole hours). Other zones, negative times and times outside the
-- lookup table keep the calendar path.
--
-- Oracle is toString(), which renders hour and minute through toDateTimeComponents, a different code path.
-- Do not use formatDateTime('%H'): it calls the same ToHourImpl, so it would compare the fast path to itself.
-- n is printed so that a generator producing no rows cannot pass as "no mismatches".

SELECT * FROM (
    WITH ts AS (
        -- epoch sweep: stride coprime with 86400, so every hour and minute residue occurs; spans 1970..2046
        SELECT toInt64(number) * 39989 AS t FROM numbers(60000)
        -- every second around the epoch, covering the local day that contains t = 0 in every zone
        UNION ALL SELECT toInt64(number) - 50400 FROM numbers(136800)
        -- before the epoch: the fast paths must not apply
        UNION ALL SELECT -toInt64(number) * 39989 FROM numbers(20000)
        -- outside the lookup table: the cctz escape path must not be affected
        UNION ALL SELECT arrayJoin([-2209000000, 10413800000, 16725303245]) + toInt64(number) * 97 FROM numbers(2000)
    )
    -- both fast paths, whole-hour offset
    SELECT 'UTC' AS zone, t >= 0 AS epoch, count() AS n,
           countIf(toHour(dt) != toUInt8(substring(s, 12, 2))) AS bad_hour,
           countIf(toMinute(dt) != toUInt8(substring(s, 15, 2))) AS bad_minute
    FROM (SELECT t, toDateTime64(t, 0, 'UTC') AS dt, toString(dt) AS s FROM ts) GROUP BY epoch
    -- both fast paths, +05:30 offset
    UNION ALL SELECT 'Asia/Kolkata', t >= 0, count(),
           countIf(toHour(dt) != toUInt8(substring(s, 12, 2))),
           countIf(toMinute(dt) != toUInt8(substring(s, 15, 2)))
    FROM (SELECT t, toDateTime64(t, 0, 'Asia/Kolkata') AS dt, toString(dt) AS s FROM ts) GROUP BY t >= 0
    -- minute fast, hour on the calendar path: +12:45 with a whole-hour DST step
    UNION ALL SELECT 'Pacific/Chatham', t >= 0, count(),
           countIf(toHour(dt) != toUInt8(substring(s, 12, 2))),
           countIf(toMinute(dt) != toUInt8(substring(s, 15, 2)))
    FROM (SELECT t, toDateTime64(t, 0, 'Pacific/Chatham') AS dt, toString(dt) AS s FROM ts) GROUP BY t >= 0
    -- minute fast, hour on the calendar path, west of UTC
    UNION ALL SELECT 'America/St_Johns', t >= 0, count(),
           countIf(toHour(dt) != toUInt8(substring(s, 12, 2))),
           countIf(toMinute(dt) != toUInt8(substring(s, 15, 2)))
    FROM (SELECT t, toDateTime64(t, 0, 'America/St_Johns') AS dt, toString(dt) AS s FROM ts) GROUP BY t >= 0
    -- minute already fast before this change, hour on the calendar path
    UNION ALL SELECT 'Europe/Berlin', t >= 0, count(),
           countIf(toHour(dt) != toUInt8(substring(s, 12, 2))),
           countIf(toMinute(dt) != toUInt8(substring(s, 15, 2)))
    FROM (SELECT t, toDateTime64(t, 0, 'Europe/Berlin') AS dt, toString(dt) AS s FROM ts) GROUP BY t >= 0
    -- neither fast path: 30-minute DST step
    UNION ALL SELECT 'Australia/Lord_Howe', t >= 0, count(),
           countIf(toHour(dt) != toUInt8(substring(s, 12, 2))),
           countIf(toMinute(dt) != toUInt8(substring(s, 15, 2)))
    FROM (SELECT t, toDateTime64(t, 0, 'Australia/Lord_Howe') AS dt, toString(dt) AS s FROM ts) GROUP BY t >= 0
    -- neither fast path: sub-hour offset that moved in 1986
    UNION ALL SELECT 'Asia/Kathmandu', t >= 0, count(),
           countIf(toHour(dt) != toUInt8(substring(s, 12, 2))),
           countIf(toMinute(dt) != toUInt8(substring(s, 15, 2)))
    FROM (SELECT t, toDateTime64(t, 0, 'Asia/Kathmandu') AS dt, toString(dt) AS s FROM ts) GROUP BY t >= 0
    -- neither fast path: sub-minute offset until 1972, inside the epoch
    UNION ALL SELECT 'Africa/Monrovia', t >= 0, count(),
           countIf(toHour(dt) != toUInt8(substring(s, 12, 2))),
           countIf(toMinute(dt) != toUInt8(substring(s, 15, 2)))
    FROM (SELECT t, toDateTime64(t, 0, 'Africa/Monrovia') AS dt, toString(dt) AS s FROM ts) GROUP BY t >= 0
) ORDER BY zone, epoch;
