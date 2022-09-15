SET force_timezone='Europe/Helsinki';

SELECT toDateTime('1999-12-12 12:12:12') - toDateTime('1999-12-12 12:12:12', 'Europe/Moscow');
SELECT toUnixTimestamp(toDateTime('1999-12-12 12:12:12')) SETTINGS force_timezone = 'Asia/Novosibirsk';
SELECT toUnixTimestamp(toDateTime('1999-12-12 12:12:12'));
SELECT toStartOfHour(toDateTime('1999-12-12 12:12:12'), 'UTC') SETTINGS force_timezone = 'America/Denver';
SELECT toUnixTimestamp64Milli(toDateTime64('1999-12-12 12:12:12.123', 3));
SELECT toUnixTimestamp64Milli(toDateTime64('1999-12-12 12:12:12.123', 3)) SETTINGS force_timezone = 'Europe/Zurich';
SELECT toTimeZone(toDateTime64('1999-12-12 12:12:12.123', 3) + toIntervalSecond(5), 'UTC') SETTINGS force_timezone = 'Europe/Zurich';