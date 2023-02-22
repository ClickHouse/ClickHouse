SET implicit_timezone = 'Asia/Novosibirsk';

SELECT toDateTime64(toDateTime64('1999-12-12 23:23:23.123', 3), 3, 'Europe/Zurich');

SELECT toDateTime64(toDateTime64('1999-12-12 23:23:23.123', 3), 3, 'Europe/Zurich') SETTINGS implicit_timezone = 'Europe/Zurich';

SET implicit_timezone = 'Europe/Zurich';

SELECT toDateTime64(toDateTime64('1999-12-12 23:23:23.123', 3), 3, 'Asia/Novosibirsk');