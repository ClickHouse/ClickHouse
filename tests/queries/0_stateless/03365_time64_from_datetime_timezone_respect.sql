SET allow_experimental_time_time64_type = 1;
SET use_legacy_to_time = 0;

SET session_timezone = 'Antarctica/DumontDUrville';
SELECT toTime64(toDateTime(1200000), 3);
SELECT toDateTime(1200000);

SET session_timezone = 'Cuba';
SELECT toTime64(reinterpret(toUInt64(12345), 'DateTime64(0)'), 3);
SELECT toTime(reinterpret(toUInt64(12345), 'DateTime64(0)'));
SELECT toDateTime64(12345, 0);

-- When DateTime has explicit timezone, toTime and toTime64 should use it, not session_timezone

SET session_timezone = 'UTC';
SELECT toTime64(toDateTime('2020-01-01 12:34:56', 'Europe/Moscow'), 3);
SELECT toTime(toDateTime('2020-01-01 12:34:56', 'Europe/Moscow'));

SET session_timezone = 'America/New_York';
SELECT toTime64(toDateTime('2020-01-01 12:34:56', 'Europe/Moscow'), 3);
SELECT toTime(toDateTime('2020-01-01 12:34:56', 'Europe/Moscow'));

-- Same for DateTime64 with explicit timezone
SET session_timezone = 'UTC';
SELECT toTime64(toDateTime64('2020-01-01 12:34:56.789', 3, 'Europe/Moscow'), 3);
SELECT toTime(toDateTime64('2020-01-01 12:34:56.789', 3, 'Europe/Moscow'));

SET session_timezone = 'America/New_York';
SELECT toTime64(toDateTime64('2020-01-01 12:34:56.789', 3, 'Europe/Moscow'), 3);
SELECT toTime(toDateTime64('2020-01-01 12:34:56.789', 3, 'Europe/Moscow'));

-- No timezone for DateTime, but session_timezone is used
SET session_timezone = 'America/New_York';
SELECT toTime64(toDateTime('2020-01-01 12:34:56'), 3);
SELECT toTime(toDateTime('2020-01-01 12:34:56'));

SET session_timezone = 'America/New_York';
SELECT toTime64(toDateTime64('2020-01-01 12:34:56.789', 3), 3);
SELECT toTime(toDateTime64('2020-01-01 12:34:56.789', 3));
