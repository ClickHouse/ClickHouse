DROP TABLE IF EXISTS test_numbers;
CREATE TABLE test_numbers (a Array(Int64)) engine=MergeTree ORDER BY a;
INSERT INTO test_numbers VALUES ([1,2,3,4,5,6]);
INSERT INTO test_numbers VALUES ([1,2,4,5]);
INSERT INTO test_numbers VALUES ([1,4,3,0,5,5,5]);
SELECT groupArrayIntersect(*) FROM test_numbers;
DROP TABLE test_numbers;

DROP TABLE IF EXISTS test_string;
CREATE TABLE test_string (a Array(String)) engine=MergeTree ORDER BY a;
INSERT INTO test_string VALUES (['a', 'b', 'c', 'd', 'e', 'f']);
INSERT INTO test_string VALUES (['a', 'aa', 'b', 'bb', 'c', 'cc', 'd', 'dd', 'f', 'ff']);
INSERT INTO test_string VALUES (['ae', 'ab', 'a', 'bb', 'c']);
SELECT groupArrayIntersect(*) FROM test_string;
DROP TABLE test_string;

DROP TABLE IF EXISTS test_datetime;
CREATE TABLE test_datetime (a Array(DateTime)) engine=MergeTree ORDER BY a;
INSERT INTO test_datetime VALUES ([toDateTime('2023-01-01 00:00:00'), toDateTime('2023-01-01 01:02:03'), toDateTime('2023-01-01 02:03:04')]);
INSERT INTO test_datetime VALUES ([toDateTime('2023-01-01 00:00:00'), toDateTime('2023-01-01 01:02:04'), toDateTime('2023-01-01 02:03:05')]);
SELECT groupArrayIntersect(*) from test_datetime;
DROP TABLE test_datetime;

DROP TABLE IF EXISTS test_date32;
CREATE TABLE test_date32 (a Array(Date32)) engine=MergeTree ORDER BY a;
INSERT INTO test_date32 VALUES ([toDate32('2023-01-01 00:00:00'), toDate32('2023-01-01 00:00:01')]);
SELECT groupArrayIntersect(*) from test_date32;
DROP TABLE test_date32;

DROP TABLE IF EXISTS test_date;
CREATE TABLE test_date (a Array(Date)) engine=MergeTree ORDER BY a;
INSERT INTO test_date VALUES ([toDate('2023-01-01 00:00:00'), toDate('2023-01-01 00:00:01')]);
SELECT groupArrayIntersect(*) from test_date;
DROP TABLE test_date;
