SET session_timezone = 'Абырвалг'; -- { serverError BAD_ARGUMENTS}

SET session_timezone = 'Asia/Novosibirsk';
SELECT toDateTime64(toDateTime64('2022-12-12 23:23:23.123', 3), 3, 'Europe/Zurich');
SELECT toDateTime64(toDateTime64('2022-12-12 23:23:23.123', 3), 3, 'Europe/Zurich') SETTINGS session_timezone = 'Europe/Zurich';

SET session_timezone = 'Asia/Manila';
SELECT toDateTime64(toDateTime64('2022-12-12 23:23:23.123', 3), 3, 'Asia/Novosibirsk');

SELECT timezone(), timezoneOf(now()) SETTINGS session_timezone = 'Europe/Zurich' FORMAT TSV;
SELECT timezone(), timezoneOf(now()) SETTINGS session_timezone = 'Pacific/Pitcairn' FORMAT TSV;
