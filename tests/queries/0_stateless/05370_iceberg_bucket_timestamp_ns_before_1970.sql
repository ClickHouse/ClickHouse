-- Iceberg floors `timestamp_ns` to microseconds before hashing, so values before 1970 with a sub-microsecond part
-- must hash like the preceding microsecond: https://github.com/ClickHouse/ClickHouse/issues/125330

SELECT icebergBucket(16, toDateTime64('1969-12-31 23:59:59.999999999', 9, 'UTC'));
SELECT icebergBucket(16, toDateTime64('1969-12-31 23:59:59.999998500', 9, 'UTC'));
SELECT icebergBucket(16, toDateTime64('1969-12-31 23:59:59.999999', 6, 'UTC'));
SELECT icebergBucket(16, fromUnixTimestamp64Nano(toInt64(-1), 'Asia/Kolkata'));
SELECT icebergHash(toDateTime64('1969-12-31 23:59:59.999999999', 9, 'UTC')) = icebergHash(toInt64(-1));

SELECT count()
FROM (SELECT toInt64(number) - 5000 AS ns FROM numbers(10000))
WHERE icebergHash(fromUnixTimestamp64Nano(ns, 'UTC')) != icebergHash(intDiv(ns - positiveModulo(ns, 1000), 1000));
