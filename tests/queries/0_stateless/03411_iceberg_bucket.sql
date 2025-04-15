-- Test is taken both from Iceberg spec (https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements) and reference Iceberg repo (https://github.com/apache/iceberg/blob/6e8718113c08aebf76d8e79a9e2534c89c73407a/api/src/test/java/org/apache/iceberg/transforms/TestBucketing.java)

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
WITH
    timestamp('1970-01-01 22:31:08') AS ts,
    toUnixTimestamp64Micro(ts) - toUnixTimestamp64Micro(toStartOfDay(ts)) AS microseconds_since_day_start
SELECT icebergHash(microseconds_since_day_start);
SELECT icebergHash('2017-11-16' :: Date);
SELECT icebergHash('2017-11-16 22:31:08' :: DateTime64(6));
SELECT icebergHash('2017-11-16T22:31:08.000001' :: DateTime64(6));
SELECT icebergHash('2017-11-16 22:31:08' :: DateTime64(9));
SELECT icebergHash('2017-11-16T22:31:08.000001' :: DateTime64(9));
SELECT icebergHash('2017-11-16T22:31:08.000001001' :: DateTime64(9));
SELECT icebergHash('iceberg');
SELECT icebergHash('iceberg' :: String);
SELECT icebergHash('iceberg' :: FixedString(7));
SELECT icebergHash('\x00\x01\x02\x03' :: FixedString(4));
SELECT icebergHash('\x00\x01\x02\x03' :: String);
SELECT icebergHash('f79c3e09-677c-4bbd-a479-3f349cb785e7' :: UUID);


