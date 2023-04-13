DROP TABLE IF EXISTS spark_bar_test;

CREATE TABLE spark_bar_test (`cnt` UInt64,`event_date` Date) ENGINE = MergeTree ORDER BY event_date SETTINGS index_granularity = 8192;

INSERT INTO spark_bar_test VALUES(1,'2020-01-01'),(4,'2020-01-02'),(5,'2020-01-03'),(2,'2020-01-04'),(3,'2020-01-05'),(7,'2020-01-06'),(6,'2020-01-07'),(8,'2020-01-08'),(2,'2020-01-11');

-- { echoOn }

SELECT sparkbar(2)(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(3)(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(4)(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(5)(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(6)(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(7)(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(8)(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(9)(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(10)(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(11)(event_date,cnt) FROM spark_bar_test;

SELECT sparkbar(11,2,5)(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(11,3,7)(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(11,4,11)(event_date,cnt) FROM spark_bar_test;

SELECT sparkbar(11,toDate('2020-01-02'),toDate('2020-01-05'))(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(11,toDate('2020-01-03'),toDate('2020-01-07'))(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(11,toDate('2020-01-04'),toDate('2020-01-11'))(event_date,cnt) FROM spark_bar_test;

SELECT sparkbar(2,toDate('2020-01-01'),toDate('2020-01-08'))(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(2,toDate('2020-01-02'),toDate('2020-01-09'))(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(3,toDate('2020-01-01'),toDate('2020-01-09'))(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(3,toDate('2020-01-01'),toDate('2020-01-10'))(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(4,toDate('2020-01-01'),toDate('2020-01-08'))(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(5,toDate('2020-01-01'),toDate('2020-01-10'))(event_date,cnt) FROM spark_bar_test;
SELECT sparkbar(9,toDate('2020-01-01'),toDate('2020-01-10'))(event_date,cnt) FROM spark_bar_test;

WITH number DIV 50 AS k, toUInt32(number % 50) AS value SELECT k, sparkbar(50, 0, 99)(number, value) FROM numbers(100) GROUP BY k ORDER BY k;

SELECT sparkbar(128, 0, 9223372036854775806)(toUInt64(9223372036854775806), number % 65535) FROM numbers(100);
SELECT sparkbar(128)(toUInt64(9223372036854775806), number % 65535) FROM numbers(100);
SELECT sparkbar(9)(x, y) FROM (SELECT * FROM Values('x UInt64, y UInt8', (18446744073709551615,255), (0,0), (0,0), (4036797895307271799,254)));

SELECT sparkbar(8, 0, 7)((number + 1) % 8, 1), sparkbar(8, 0, 7)((number + 2) % 8, 1), sparkbar(8, 0, 7)((number + 3) % 8, 1) FROM numbers(7);

SELECT sparkbar(2)(number, -number) FROM numbers(10);
SELECT sparkbar(10)(number, number - 7) FROM numbers(10);
SELECT sparkbar(1024)(number, number) FROM numbers(1024);
SELECT sparkbar(1024)(number, 1) FROM numbers(1024);
SELECT sparkbar(1024)(number, 0) FROM numbers(1024);

-- { echoOff }

SELECT sparkbar(0)(number, number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT sparkbar(1)(number, number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT sparkbar(1025)(number, number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT sparkbar(2, 10, 9)(number, number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT sparkbar(2, -5, -1)(number, number) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT sparkbar(2, -5, 1)(number, number) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT sparkbar(2)(toInt32(number),  number) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT sparkbar(2, 0)(number, number) FROM numbers(10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT sparkbar(2, 0, 5, 8)(number, number) FROM numbers(10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- it causes overflow, just check that it doesn't crash under UBSan, do not check the result it's not really reasonable
SELECT sparkbar(10)(number, toInt64(number)) FROM numbers(toUInt64(9223372036854775807), 20) FORMAT Null;
SELECT sparkbar(10)(number, -number) FROM numbers(toUInt64(9223372036854775807), 7) FORMAT Null;
SELECT sparkbar(10)(number, number) FROM numbers(18446744073709551615, 7) FORMAT Null;
SELECT sparkbar(16)(number, number) FROM numbers(18446744073709551600, 16) FORMAT Null;

DROP TABLE IF EXISTS spark_bar_test;
