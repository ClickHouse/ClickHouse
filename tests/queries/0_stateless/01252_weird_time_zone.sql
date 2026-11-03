SELECT 'Pacific/Kiritimati', toDateTime('2020-01-02 03:04:05', 'Pacific/Kiritimati') AS x, toStartOfDay(x), toHour(x);
SELECT 'Africa/El_Aaiun', toDateTime('2020-01-02 03:04:05', 'Africa/El_Aaiun') AS x, toStartOfDay(x), toHour(x);
SELECT 'Asia/Pyongyang', toDateTime('2020-01-02 03:04:05', 'Asia/Pyongyang') AS x, toStartOfDay(x), toHour(x);
SELECT 'Pacific/Kwajalein', toDateTime('2020-01-02 03:04:05', 'Pacific/Kwajalein') AS x, toStartOfDay(x), toHour(x);
SELECT 'Pacific/Apia', toDateTime('2020-01-02 03:04:05', 'Pacific/Apia') AS x, toStartOfDay(x), toHour(x);
SELECT 'Pacific/Enderbury', toDateTime('2020-01-02 03:04:05', 'Pacific/Enderbury') AS x, toStartOfDay(x), toHour(x);
SELECT 'Pacific/Fakaofo', toDateTime('2020-01-02 03:04:05', 'Pacific/Fakaofo') AS x, toStartOfDay(x), toHour(x);

SELECT 'Pacific/Kiritimati', rand() as r, toHour(toDateTime(r, 'Pacific/Kiritimati') AS t) AS h, t, toTypeName(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Africa/El_Aaiun', rand() as r, toHour(toDateTime(r, 'Africa/El_Aaiun') AS t) AS h, t, toTypeName(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Asia/Pyongyang', rand() as r, toHour(toDateTime(r, 'Asia/Pyongyang') AS t) AS h, t, toTypeName(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Pacific/Kwajalein', rand() as r, toHour(toDateTime(r, 'Pacific/Kwajalein') AS t) AS h, t, toTypeName(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Pacific/Apia', rand() as r, toHour(toDateTime(r, 'Pacific/Apia') AS t) AS h, t, toTypeName(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Pacific/Enderbury', rand() as r, toHour(toDateTime(r, 'Pacific/Enderbury') AS t) AS h, t, toTypeName(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Pacific/Fakaofo', rand() as r, toHour(toDateTime(r, 'Pacific/Fakaofo') AS t) AS h, t, toTypeName(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;

-- timezoneOffset must report the true UTC offset, including its whole-day component (issue #115281).
SELECT '--- offset of zones that crossed the international date line';
SELECT 'Pacific/Kiritimati', timeZoneOffset(toDateTime('2025-06-15 12:00:00', 'Pacific/Kiritimati'));
SELECT 'Pacific/Apia', timeZoneOffset(toDateTime('2025-06-15 12:00:00', 'Pacific/Apia'));
SELECT 'Pacific/Fakaofo', timeZoneOffset(toDateTime('2025-06-15 12:00:00', 'Pacific/Fakaofo'));
SELECT 'Pacific/Enderbury', timeZoneOffset(toDateTime('2025-06-15 12:00:00', 'Pacific/Enderbury'));
SELECT 'Pacific/Kanton', timeZoneOffset(toDateTime('2025-06-15 12:00:00', 'Pacific/Kanton'));
SELECT 'Pacific/Kwajalein', timeZoneOffset(toDateTime('2025-06-15 12:00:00', 'Pacific/Kwajalein'));
SELECT 'Kwajalein', timeZoneOffset(toDateTime('2025-06-15 12:00:00', 'Kwajalein'));

SELECT '--- the same zones before their jump, and a +13:45 zone, must not move';
SELECT 'Pacific/Apia', timeZoneOffset(toDateTime('2010-01-01 12:00:00', 'Pacific/Apia'));
SELECT 'Pacific/Fakaofo', timeZoneOffset(toDateTime('2010-01-01 12:00:00', 'Pacific/Fakaofo'));
SELECT 'Pacific/Chatham', timeZoneOffset(toDateTime('2010-01-01 12:00:00', 'Pacific/Chatham'));
SELECT 'Pacific/Chatham', timeZoneOffset(toDateTime('2025-06-15 12:00:00', 'Pacific/Chatham'));

SELECT '--- negative offsets before the epoch';
SELECT 'Africa/Abidjan', timeZoneOffset(toDateTime64(-1900000000, 0, 'Africa/Abidjan'));
SELECT 'Africa/Accra', timeZoneOffset(toDateTime64(-1900000000, 0, 'Africa/Accra'));
SELECT 'Africa/Addis_Ababa', timeZoneOffset(toDateTime64(-1900000000, 0, 'Africa/Addis_Ababa'));
SELECT 'Europe/Moscow', timeZoneOffset(toDateTime64(-1900000000, 0, 'Europe/Moscow'));

SELECT '--- parseDateTime agrees with toDateTime on an affected zone';
SELECT toUnixTimestamp(parseDateTime('2025-06-15 12:00:00', '%Y-%m-%d %H:%i:%S', 'Pacific/Kiritimati')) = toUnixTimestamp(toDateTime('2025-06-15 12:00:00', 'Pacific/Kiritimati'));
SELECT toUnixTimestamp(parseDateTimeOrNull('2025-06-15 12:00:00', '%Y-%m-%d %H:%i:%S', 'Pacific/Kiritimati')) = toUnixTimestamp(toDateTime('2025-06-15 12:00:00', 'Pacific/Kiritimati'));
SELECT toUnixTimestamp(parseDateTime64('2025-06-15 12:00:00', '%Y-%m-%d %H:%i:%S', 'Pacific/Kiritimati')) = toUnixTimestamp(toDateTime('2025-06-15 12:00:00', 'Pacific/Kiritimati'));
SELECT toUnixTimestamp(parseDateTimeInJodaSyntax('2025-06-15 12:00:00', 'yyyy-MM-dd HH:mm:ss', 'Pacific/Kiritimati')) = toUnixTimestamp(toDateTime('2025-06-15 12:00:00', 'Pacific/Kiritimati'));
SELECT toUnixTimestamp(parseDateTimeBestEffort('2025-06-15 12:00:00', 'Pacific/Kiritimati')) = toUnixTimestamp(toDateTime('2025-06-15 12:00:00', 'Pacific/Kiritimati'));
SELECT toUnixTimestamp(parseDateTimeInJodaSyntax('2025-06-15 12:00:00 Pacific/Kiritimati', 'yyyy-MM-dd HH:mm:ss z')) = toUnixTimestamp(toDateTime('2025-06-15 12:00:00', 'Pacific/Kiritimati'));

SELECT '--- other consumers of the offset';
SELECT EXTRACT(TIMEZONE_HOUR FROM toDateTime('2025-06-15 12:00:00', 'Pacific/Kiritimati')), EXTRACT(TIMEZONE_MINUTE FROM toDateTime('2025-06-15 12:00:00', 'Pacific/Kiritimati'));
SELECT formatDateTime(toDateTime64(1750000000, 0, 'UTC'), '%z', 'Pacific/Kiritimati');
SELECT toUTCTimestamp(CAST('2025-06-15 12:00:00' AS DateTime), 'Pacific/Kiritimati'), fromUTCTimestamp(CAST('2025-06-15 12:00:00' AS DateTime), 'Pacific/Kiritimati');
SELECT toUTCTimestamp(CAST('2025-06-15 12:00:00.500' AS DateTime64(3)), 'Pacific/Kiritimati'), fromUTCTimestamp(CAST('2025-06-15 12:00:00.500' AS DateTime64(3)), 'Pacific/Kiritimati');

SELECT '--- consumers that reduce the offset modulo a day must be unaffected';
SELECT CAST(toDateTime('2025-06-15 12:00:00', 'Pacific/Kiritimati') AS Time), CAST(toDateTime64('2025-06-15 12:00:00.5', 3, 'Pacific/Kiritimati') AS Time64(3));
SELECT toTime(toDateTime('2025-06-15 12:00:00', 'Pacific/Kiritimati')), toStartOfDay(toDateTime('2025-06-15 12:00:00', 'Pacific/Kiritimati')), toHour(toDateTime('2025-06-15 12:00:00', 'Pacific/Kiritimati')), toDate(toDateTime('2025-06-15 12:00:00', 'Pacific/Kiritimati'));

SELECT '--- daylight saving transitions keep their sub-day offset';
SELECT timeZoneOffset(toDateTime('2025-03-09 01:00:00', 'America/New_York')), timeZoneOffset(toDateTime('2025-03-09 03:00:00', 'America/New_York')), timeZoneOffset(toDateTime('2025-11-02 03:00:00', 'America/New_York'));

SELECT '--- inside and outside the lookup table window report the same offset';
SELECT timeZoneOffset(toDateTime64(1750000000, 0, 'Pacific/Kiritimati')) = timeZoneOffset(toDateTime64(90000000000, 0, 'Pacific/Kiritimati'));

SELECT '--- every zone agrees with rendering the instant as local text and reading it back as UTC';
WITH
    arrayJoin([toDateTime64(1262347200, 0, 'UTC'), toDateTime64(1750000000, 0, 'UTC'), toDateTime64(2000000000, 0, 'UTC')]) AS ts,
    formatDateTime(ts, '%z', time_zone) AS z,
    (toInt64(substring(z, 2, 2)) * 3600 + toInt64(substring(z, 4, 2)) * 60) * multiIf(substring(z, 1, 1) = '-', -1, 1) AS reported,
    toInt64(toDateTime(formatDateTime(ts, '%Y-%m-%d %H:%i:%S', time_zone), 'UTC')) - toInt64(ts) AS expected
SELECT count() FROM system.time_zones WHERE reported != expected;
