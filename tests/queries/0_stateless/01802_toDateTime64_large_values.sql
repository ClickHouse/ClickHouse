-- { echo }

SELECT toDateTime64('2205-12-12 12:12:12', 0, 'UTC');
SELECT toDateTime64('2205-12-12 12:12:12', 0, 'Asia/Istanbul');

SELECT toDateTime64('2205-12-12 12:12:12', 6, 'Asia/Istanbul');
SELECT toDateTime64('2205-12-12 12:12:12', 6, 'Asia/Istanbul');