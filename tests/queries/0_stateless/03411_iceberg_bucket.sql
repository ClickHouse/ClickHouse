-- Tags: no-random-settings,
-- Test is taken both from Iceberg spec (https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements) and reference Iceberg repo (https://github.com/apache/iceberg/blob/6e8718113c08aebf76d8e79a9e2534c89c73407a/api/src/test/java/org/apache/iceberg/transforms/TestBucketing.java)
SELECT 'icebergHash';
SELECT icebergHash(true);
SELECT icebergHash(1);
SELECT icebergHash(0.0 :: Float32);
SELECT icebergHash(-0.0 :: Float32);
SELECT icebergHash(1.0 :: Float32);
SELECT icebergHash(0.0 :: Float64);
SELECT icebergHash(-0.0 :: Float64);
SELECT icebergHash(1.0 :: Float64);
SELECT icebergHash(34 :: Int32);
SELECT icebergHash(34 :: UInt32);
SELECT icebergHash(34 :: Int64);
SELECT icebergHash(34 :: UInt64);
SELECT icebergHash(14.20 :: Decimal32(2));
SELECT icebergHash(14.20 :: Decimal64(2));
SELECT icebergHash(14.20 :: Decimal128(2));
SELECT icebergHash('2017-11-16' :: Date);
WITH
    toDateTime64('1970-01-01 22:31:08', 6, 'UTC') AS ts,
    toUnixTimestamp64Micro(ts) AS microseconds_since_day_start
SELECT icebergHash(microseconds_since_day_start);
SELECT icebergHash(toDateTime64('2017-11-16T22:31:08', 6, 'UTC'));
SELECT icebergHash(toDateTime64('2017-11-16T22:31:08.000001', 6, 'UTC'));
SELECT icebergHash(toDateTime64('2017-11-16T22:31:08', 9, 'UTC'));
SELECT icebergHash(toDateTime64('2017-11-16T22:31:08.000001', 9, 'UTC'));
SELECT icebergHash(toDateTime64('2017-11-16T22:31:08.000001001', 9, 'UTC'));
SELECT icebergHash('iceberg');
SELECT icebergHash('iceberg' :: String);
SELECT icebergHash('iceberg' :: FixedString(7));
SELECT icebergHash('\x00\x01\x02\x03' :: FixedString(4));
SELECT icebergHash('\x00\x01\x02\x03' :: String);
SELECT icebergHash('f79c3e09-677c-4bbd-a479-3f349cb785e7' :: UUID);

SELECT 'icebergBucket, modulo 5';
SELECT icebergBucket(5, true);
SELECT icebergBucket(5, 1);
SELECT icebergBucket(5, 0.0 :: Float32);
SELECT icebergBucket(5, -0.0 :: Float32);
SELECT icebergBucket(5, 1.0 :: Float32);
SELECT icebergBucket(5, 0.0 :: Float64);
SELECT icebergBucket(5, -0.0 :: Float64);
SELECT icebergBucket(5, 1.0 :: Float64);
SELECT icebergBucket(5, 34 :: Int32);
SELECT icebergBucket(5, 34 :: UInt32);
SELECT icebergBucket(5, 34 :: Int64);
SELECT icebergBucket(5, 34 :: UInt64);
SELECT icebergBucket(5, 14.20 :: Decimal32(2));
SELECT icebergBucket(5, 14.20 :: Decimal64(2));
SELECT icebergBucket(5, 14.20 :: Decimal128(2));
SELECT icebergBucket(5, '2017-11-16' :: Date);
WITH
    toDateTime64('1970-01-01 22:31:08', 6, 'UTC') AS ts,
    toUnixTimestamp64Micro(ts) AS microseconds_since_day_start
SELECT icebergBucket(5, microseconds_since_day_start);
SELECT icebergBucket(5, toDateTime64('2017-11-16T22:31:08', 6, 'UTC'));
SELECT icebergBucket(5, toDateTime64('2017-11-16T22:31:08.000001', 6, 'UTC'));
SELECT icebergBucket(5, toDateTime64('2017-11-16T22:31:08', 9, 'UTC'));
SELECT icebergBucket(5, toDateTime64('2017-11-16T22:31:08.000001', 9, 'UTC'));
SELECT icebergBucket(5, toDateTime64('2017-11-16T22:31:08.000001001', 9, 'UTC'));
SELECT icebergBucket(5, 'iceberg');
SELECT icebergBucket(5, 'iceberg' :: String);
SELECT icebergBucket(5, 'iceberg' :: FixedString(7));
SELECT icebergBucket(5, '\x00\x01\x02\x03' :: FixedString(4));
SELECT icebergBucket(5, '\x00\x01\x02\x03' :: String);
SELECT icebergBucket(5, 'f79c3e09-677c-4bbd-a479-3f349cb785e7' :: UUID);

SELECT 'icebergBucket, modulo 13';
SELECT icebergBucket(13, true);
SELECT icebergBucket(13, 1);
SELECT icebergBucket(13, 0.0 :: Float32);
SELECT icebergBucket(13, -0.0 :: Float32);
SELECT icebergBucket(13, 1.0 :: Float32);
SELECT icebergBucket(13, 0.0 :: Float64);
SELECT icebergBucket(13, -0.0 :: Float64);
SELECT icebergBucket(13, 1.0 :: Float64);
SELECT icebergBucket(13, 34 :: Int32);
SELECT icebergBucket(13, 34 :: UInt32);
SELECT icebergBucket(13, 34 :: Int64);
SELECT icebergBucket(13, 34 :: UInt64);
SELECT icebergBucket(13, 14.20 :: Decimal32(2));
SELECT icebergBucket(13, 14.20 :: Decimal64(2));
SELECT icebergBucket(13, 14.20 :: Decimal128(2));
SELECT icebergBucket(13, '2017-11-16' :: Date);
WITH
    toDateTime64('1970-01-01 22:31:08', 6, 'UTC') AS ts,
    toUnixTimestamp64Micro(ts) AS microseconds_since_day_start
SELECT icebergBucket(13, microseconds_since_day_start);
SELECT icebergBucket(13, toDateTime64('2017-11-16T22:31:08', 6, 'UTC'));
SELECT icebergBucket(13, toDateTime64('2017-11-16T22:31:08.000001', 6, 'UTC'));
SELECT icebergBucket(13, toDateTime64('2017-11-16T22:31:08', 9, 'UTC'));
SELECT icebergBucket(13, toDateTime64('2017-11-16T22:31:08.000001', 9, 'UTC'));
SELECT icebergBucket(13, toDateTime64('2017-11-16T22:31:08.000001001', 9, 'UTC'));
SELECT icebergBucket(13, 'iceberg');
SELECT icebergBucket(13, 'iceberg' :: String);
SELECT icebergBucket(13, 'iceberg' :: FixedString(7));
SELECT icebergBucket(13, '\x00\x01\x02\x03' :: FixedString(4));
SELECT icebergBucket(13, '\x00\x01\x02\x03' :: String);
SELECT icebergBucket(13, 'f79c3e09-677c-4bbd-a479-3f349cb785e7' :: UUID);