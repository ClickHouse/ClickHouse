-- { echo }

SELECT formatDateTime(toDateTime64('1935-12-12 12:12:12', 0, 'Europe/Moscow'), '%C');
SELECT formatDateTime(toDateTime64('1969-12-12 12:12:12', 0, 'Europe/Moscow'), '%C');
SELECT formatDateTime(toDateTime64('1989-12-12 12:12:12', 0, 'Europe/Moscow'), '%C');
SELECT formatDateTime(toDateTime64('2019-09-16 19:20:12', 0, 'Europe/Moscow'), '%C');
SELECT formatDateTime(toDateTime64('2105-12-12 12:12:12', 0, 'Europe/Moscow'), '%C');
SELECT formatDateTime(toDateTime64('2205-12-12 12:12:12', 0, 'Europe/Moscow'), '%C');

-- non-zero scale
SELECT formatDateTime(toDateTime64('1935-12-12 12:12:12', 6, 'Europe/Moscow'), '%C');
SELECT formatDateTime(toDateTime64('1969-12-12 12:12:12', 6, 'Europe/Moscow'), '%C');
SELECT formatDateTime(toDateTime64('1989-12-12 12:12:12', 6, 'Europe/Moscow'), '%C');
SELECT formatDateTime(toDateTime64('2019-09-16 19:20:12', 0, 'Europe/Moscow'), '%C');
SELECT formatDateTime(toDateTime64('2105-12-12 12:12:12', 6, 'Europe/Moscow'), '%C');
SELECT formatDateTime(toDateTime64('2205-01-12 12:12:12', 6, 'Europe/Moscow'), '%C');