SELECT toUnixTimestampEx(makeDate(2023, 5, 10));
SELECT toUnixTimestampEx(makeDate32(2023, 5, 10));
SELECT toUnixTimestampEx(makeDate(2023, 5, 10), 'Pacific/Auckland');
SELECT toUnixTimestampEx(makeDate32(2023, 5, 10), 'Pacific/Auckland');
SELECT toUnixTimestampEx(toDateTime64('1928-12-31 12:12:12.123', 3, 'UTC'));
SELECT toUnixTimestampEx('1970-01-01 00:00:00', 'UTC');
SELECT toUnixTimestampEx(materialize('1970-01-01 00:00:00'), 'UTC');
SELECT toUnixTimestampEx('1970-01-01 00:00:00', 'Asia/Shanghai');
SELECT toUnixTimestampEx(materialize('1970-01-01 00:00:00'), 'Asia/Shanghai');
