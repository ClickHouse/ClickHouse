CREATE TABLE table_02184 (x UInt8); --{serverError 119}
SET default_table_engine = 'Log';
CREATE TABLE table_02184 (x UInt8);
SHOW CREATE TABLE table_02184;
DROP TABLE table_02184;
SET default_table_engine = 'MergeTree';
CREATE TABLE table_02184 (x UInt8); --{serverError 42}
CREATE TABLE table_02184 (x UInt8, PRIMARY KEY (x));
SHOW CREATE TABLE table_02184;
DROP TABLE table_02184;
CREATE TABLE test_optimize_exception (date Date) PARTITION BY toYYYYMM(date) ORDER BY date;
SHOW CREATE TABLE test_optimize_exception;
DROP TABLE test_optimize_exception;
CREATE TABLE table_02184 (x UInt8) PARTITION BY x; --{serverError 36}
CREATE TABLE table_02184 (x UInt8) ORDER BY x;
SHOW CREATE TABLE table_02184;
DROP TABLE table_02184;
CREATE TABLE table_02184 (x UInt8) PRIMARY KEY x;
SHOW CREATE TABLE table_02184;
DROP TABLE table_02184;
SET default_table_engine = 'Memory';
CREATE TABLE numbers1 AS SELECT number FROM numbers(1000);
SHOW CREATE TABLE numbers1;
SELECT * FROM numbers1;
DROP TABLE numbers1;
SET default_table_engine = 'MergeTree';
CREATE TABLE numbers2 ORDER BY intHash32(number) SAMPLE BY intHash32(number) AS SELECT number FROM numbers(10);
SELECT * FROM numbers2;
SHOW CREATE TABLE numbers2;
DROP TABLE numbers2;
CREATE TABLE test_table (EventDate Date, CounterID UInt32,  UserID UInt64,  EventTime DateTime('Europe/Moscow'), UTCEventTime DateTime('UTC')) PARTITION BY EventDate PRIMARY KEY CounterID;
SET default_table_engine = 'Memory';
CREATE MATERIALIZED VIEW test_view (Rows UInt64,  MaxHitTime DateTime('Europe/Moscow')) AS SELECT count() AS Rows, max(UTCEventTime) AS MaxHitTime FROM test_table;
CREATE MATERIALIZED VIEW test_view_filtered (EventDate Date, CounterID UInt32) POPULATE AS SELECT CounterID, EventDate FROM test_table WHERE EventDate < '2013-01-01';
INSERT INTO test_table (EventDate, UTCEventTime) VALUES ('2014-01-02', '2014-01-02 03:04:06');

SELECT * FROM test_table;
SELECT * FROM test_view;
SELECT * FROM test_view_filtered;

DROP TABLE test_table;
DROP TABLE test_view;
DROP TABLE test_view_filtered;