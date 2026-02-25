SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

DROP TABLE IF EXISTS test_empty;
CREATE TABLE test_empty (a Array(Int64)) engine=MergeTree ORDER BY a;
INSERT INTO test_empty VALUES ([]);
SELECT groupArrayIntersect(*) FROM test_empty;
INSERT INTO test_empty VALUES ([1]);
SELECT groupArrayIntersect(*) FROM test_empty;
DROP TABLE test_empty;

DROP TABLE IF EXISTS test_null;
CREATE TABLE test_null (a Array(Nullable(Int64))) engine=MergeTree ORDER BY a SETTINGS allow_nullable_key=1;
INSERT INTO test_null VALUES ([NULL, NULL]);
SELECT groupArrayIntersect(*) FROM test_null;
INSERT INTO test_null VALUES ([NULL]);
SELECT groupArrayIntersect(*) FROM test_null;
INSERT INTO test_null VALUES ([1,2]);
SELECT groupArrayIntersect(*) FROM test_null;
DROP TABLE test_null;

DROP TABLE IF EXISTS test_nested_arrays;
CREATE TABLE test_nested_arrays (a Array(Array(Int64))) engine=MergeTree ORDER BY a;
INSERT INTO test_nested_arrays VALUES ([[1,2,3,4,5,6], [1,2,4,5]]);
INSERT INTO test_nested_arrays VALUES ([[1,2,4,5]]);
SELECT groupArrayIntersect(*) FROM test_nested_arrays;
INSERT INTO test_nested_arrays VALUES ([[1,4,3,0,5,5,5]]);
SELECT groupArrayIntersect(*) FROM test_nested_arrays;
DROP TABLE test_nested_arrays;

DROP TABLE IF EXISTS test_numbers;
CREATE TABLE test_numbers (a Array(Int64)) engine=MergeTree ORDER BY a;
INSERT INTO test_numbers VALUES ([1,2,3,4,5,6]);
INSERT INTO test_numbers VALUES ([1,2,4,5]);
INSERT INTO test_numbers VALUES ([1,4,3,0,5,5,5]);
SELECT groupArrayIntersect(*) FROM test_numbers;
INSERT INTO test_numbers VALUES ([9]);
SELECT groupArrayIntersect(*) FROM test_numbers;
DROP TABLE test_numbers;

DROP TABLE IF EXISTS test_big_numbers_sep;
CREATE TABLE test_big_numbers_sep (a Array(Int64)) engine=MergeTree ORDER BY a;
INSERT INTO test_big_numbers_sep SELECT array(number) FROM numbers_mt(100000);
SELECT groupArrayIntersect(*) FROM test_big_numbers_sep;
DROP TABLE test_big_numbers_sep;

DROP TABLE IF EXISTS test_big_numbers;
CREATE TABLE test_big_numbers (a Array(Int64)) engine=MergeTree ORDER BY a;
INSERT INTO test_big_numbers SELECT range(100000);
SELECT length(groupArrayIntersect(*)) FROM test_big_numbers;
INSERT INTO test_big_numbers SELECT range(99999);
SELECT length(groupArrayIntersect(*)) FROM test_big_numbers;
INSERT INTO test_big_numbers VALUES ([9]);
SELECT groupArrayIntersect(*) FROM test_big_numbers;
DROP TABLE test_big_numbers;

DROP TABLE IF EXISTS test_string;
CREATE TABLE test_string (a Array(String)) engine=MergeTree ORDER BY a;
INSERT INTO test_string VALUES (['a', 'b', 'c', 'd', 'e', 'f']);
INSERT INTO test_string VALUES (['a', 'aa', 'b', 'bb', 'c', 'cc', 'd', 'dd', 'f', 'ff']);
INSERT INTO test_string VALUES (['ae', 'ab', 'a', 'bb', 'c']);
SELECT groupArrayIntersect(*) FROM test_string;
DROP TABLE test_string;

DROP TABLE IF EXISTS test_big_string;
CREATE TABLE test_big_string (a Array(String)) engine=MergeTree ORDER BY a;
INSERT INTO test_big_string SELECT groupArray(toString(number)) FROM numbers_mt(50000);
SELECT length(groupArrayIntersect(*)) FROM test_big_string;
INSERT INTO test_big_string SELECT groupArray(toString(number)) FROM numbers_mt(49999);
SELECT length(groupArrayIntersect(*)) FROM test_big_string;
INSERT INTO test_big_string VALUES (['1']);
SELECT groupArrayIntersect(*) FROM test_big_string;
INSERT INTO test_big_string VALUES (['a']);
SELECT groupArrayIntersect(*) FROM test_big_string;
DROP TABLE test_big_string;

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
