SET timezone = 'Asia/Novosibirsk';

SELECT toDateTime64(toDateTime64('1999-12-12 23:23:23.123', 3), 3, 'Europe/Zurich');

SELECT toDateTime64(toDateTime64('1999-12-12 23:23:23.123', 3), 3, 'Europe/Zurich') SETTINGS timezone = 'Europe/Zurich';

SET timezone = 'Europe/Zurich';

SELECT toDateTime64(toDateTime64('1999-12-12 23:23:23.123', 3), 3, 'Asia/Novosibirsk');