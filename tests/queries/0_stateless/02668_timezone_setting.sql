SET timezone = 'Абырвалг'; -- { serverError BAD_ARGUMENTS}

SET timezone = 'Asia/Novosibirsk';
SELECT toDateTime64(toDateTime64('1999-12-12 23:23:23.123', 3), 3, 'Europe/Zurich');
SELECT toDateTime64(toDateTime64('1999-12-12 23:23:23.123', 3), 3, 'Europe/Zurich') SETTINGS timezone = 'Europe/Zurich';

SET timezone = 'Asia/Manila';
SELECT toDateTime64(toDateTime64('1999-12-12 23:23:23.123', 3), 3, 'Asia/Novosibirsk');

SELECT timezone(), serverTimeZone(), timezoneOf(now()) SETTINGS timezone = 'Europe/Zurich';
SELECT timezone(), serverTimeZone(), timezoneOf(now()) SETTINGS timezone = 'Pacific/Pitcairn';
