-- { echo }

SELECT toDateTime64('2205-12-12 12:12:12', 0, 'UTC');
SELECT toDateTime64('2205-12-12 12:12:12', 0, 'Europe/Moscow');

SELECT toDateTime64('2205-12-12 12:12:12', 6, 'Europe/Moscow');
SELECT toDateTime64('2205-12-12 12:12:12', 6, 'Europe/Moscow');