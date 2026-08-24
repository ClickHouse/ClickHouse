select toUnixTimestamp64Milli(toDateTime64('1969-12-31 23:59:59.999', 3, 'Europe/Amsterdam'));
select toUnixTimestamp64Milli(toDateTime64('1969-12-31 23:59:59.999', 3, 'UTC'));
select fromUnixTimestamp64Milli(toInt64(-1), 'Europe/Amsterdam');
select fromUnixTimestamp64Milli(toInt64(-1), 'UTC');
