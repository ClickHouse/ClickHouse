CREATE TABLE raw (v Int64) ENGINE = Memory;
INSERT INTO raw VALUES (-9223372036854775808), (-1234567891), (-1000000000), (-1), (0), (1234567891), (9223372036854775807);

SELECT 0, v, toMillisecond(reinterpret(v, 'DateTime64(0)') AS d), toMicrosecond(d), toNanosecond(d) FROM raw ORDER BY v;
SELECT 1, v, toMillisecond(reinterpret(v, 'DateTime64(1)') AS d), toMicrosecond(d), toNanosecond(d) FROM raw ORDER BY v;
SELECT 2, v, toMillisecond(reinterpret(v, 'DateTime64(2)') AS d), toMicrosecond(d), toNanosecond(d) FROM raw ORDER BY v;
SELECT 3, v, toMillisecond(reinterpret(v, 'DateTime64(3)') AS d), toMicrosecond(d), toNanosecond(d) FROM raw ORDER BY v;
SELECT 4, v, toMillisecond(reinterpret(v, 'DateTime64(4)') AS d), toMicrosecond(d), toNanosecond(d) FROM raw ORDER BY v;
SELECT 5, v, toMillisecond(reinterpret(v, 'DateTime64(5)') AS d), toMicrosecond(d), toNanosecond(d) FROM raw ORDER BY v;
SELECT 6, v, toMillisecond(reinterpret(v, 'DateTime64(6)') AS d), toMicrosecond(d), toNanosecond(d) FROM raw ORDER BY v;
SELECT 7, v, toMillisecond(reinterpret(v, 'DateTime64(7)') AS d), toMicrosecond(d), toNanosecond(d) FROM raw ORDER BY v;
SELECT 8, v, toMillisecond(reinterpret(v, 'DateTime64(8)') AS d), toMicrosecond(d), toNanosecond(d) FROM raw ORDER BY v;
SELECT 9, v, toMillisecond(reinterpret(v, 'DateTime64(9)') AS d), toMicrosecond(d), toNanosecond(d) FROM raw ORDER BY v;

SELECT toMillisecond(reinterpret(-1234567891::Int64, 'DateTime64(4)')), toMicrosecond(reinterpret(-1234567891::Int64, 'DateTime64(8)')), toNanosecond(reinterpret(1234567891::Int64, 'DateTime64(2)'));

DROP TABLE raw;
