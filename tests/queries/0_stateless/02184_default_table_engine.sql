SET default_table_engine = 'None';

CREATE TABLE table_02184 (x UInt8); --{serverError ENGINE_REQUIRED}
SET default_table_engine = 'Log';
CREATE TABLE table_02184 (x UInt8);
SHOW CREATE TABLE table_02184;
DROP TABLE table_02184;

SET default_table_engine = 'MergeTree';
CREATE TABLE table_02184 (x UInt8); --{serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}
CREATE TABLE table_02184 (x UInt8, PRIMARY KEY (x));
SHOW CREATE TABLE table_02184;
DROP TABLE table_02184;

CREATE TABLE test_optimize_exception (date Date) PARTITION BY toYYYYMM(date) ORDER BY date;
SHOW CREATE TABLE test_optimize_exception;
DROP TABLE test_optimize_exception;
CREATE TABLE table_02184 (x UInt8) PARTITION BY x; --{serverError BAD_ARGUMENTS}
CREATE TABLE table_02184 (x UInt8) ORDER BY x;
SHOW CREATE TABLE table_02184;
DROP TABLE table_02184;

CREATE TABLE table_02184 (x UInt8) PRIMARY KEY x;
SHOW CREATE TABLE table_02184;
DROP TABLE table_02184;
SET default_table_engine = 'Memory';
CREATE TABLE numbers1 AS SELECT number FROM numbers(10);
SHOW CREATE TABLE numbers1;
SELECT avg(number) FROM numbers1;
DROP TABLE numbers1;

SET default_table_engine = 'MergeTree';
CREATE TABLE numbers2 ORDER BY intHash32(number) SAMPLE BY intHash32(number) AS SELECT number FROM numbers(10);
SELECT sum(number) FROM numbers2;
SHOW CREATE TABLE numbers2;
DROP TABLE numbers2;

CREATE TABLE numbers3 ENGINE = Log AS SELECT number FROM numbers(10);
SELECT sum(number) FROM numbers3;
SHOW CREATE TABLE numbers3;
DROP TABLE numbers3;

CREATE TABLE test_table (EventDate Date, CounterID UInt32,  UserID UInt64,  EventTime DateTime('America/Los_Angeles'), UTCEventTime DateTime('UTC')) PARTITION BY EventDate PRIMARY KEY CounterID;
SET default_table_engine = 'Memory';
CREATE MATERIALIZED VIEW test_view (Rows UInt64,  MaxHitTime DateTime('America/Los_Angeles')) AS SELECT count() AS Rows, max(UTCEventTime) AS MaxHitTime FROM test_table;
CREATE MATERIALIZED VIEW test_view_filtered (EventDate Date, CounterID UInt32) POPULATE AS SELECT CounterID, EventDate FROM test_table WHERE EventDate < '2013-01-01';
SHOW CREATE TABLE test_view_filtered;
INSERT INTO test_table (EventDate, UTCEventTime) VALUES ('2014-01-02', '2014-01-02 03:04:06');

SELECT * FROM test_table;
SELECT * FROM test_view;
SELECT * FROM test_view_filtered;

DROP TABLE test_view;
DROP TABLE test_view_filtered;

SET default_table_engine = 'MergeTree';
CREATE MATERIALIZED VIEW test_view ORDER BY Rows AS SELECT count() AS Rows, max(UTCEventTime) AS MaxHitTime FROM test_table;
SET default_table_engine = 'Memory';
CREATE TABLE t1 AS test_view;
CREATE TABLE t2 ENGINE=Memory AS test_view;
SHOW CREATE TABLE t1;
SHOW CREATE TABLE t2;
DROP TABLE test_view;
DROP TABLE test_table;
DROP TABLE t1;
DROP TABLE t2;


CREATE DATABASE test_02184 ORDER BY kek; -- {serverError INCORRECT_QUERY}
CREATE DATABASE test_02184 SETTINGS x=1; -- {serverError UNKNOWN_SETTING}
CREATE TABLE table_02184 (x UInt8, y int, PRIMARY KEY (x)) ENGINE=MergeTree PRIMARY KEY y; -- {clientError BAD_ARGUMENTS}
SET default_table_engine = 'MergeTree';
CREATE TABLE table_02184 (x UInt8, y int, PRIMARY KEY (x)) PRIMARY KEY y; -- {clientError BAD_ARGUMENTS}

CREATE TABLE mt (a UInt64, b Nullable(String), PRIMARY KEY (a, coalesce(b, 'test')), INDEX b_index b TYPE set(123) GRANULARITY 1);
SHOW CREATE TABLE mt;
SET default_table_engine = 'Log';
CREATE TABLE mt2 AS mt;
SHOW CREATE TABLE mt2;
DROP TABLE mt;

SET default_table_engine = 'Log';
CREATE TEMPORARY TABLE tmp (n int);
SHOW CREATE TEMPORARY TABLE tmp;
CREATE TEMPORARY TABLE tmp1 (n int) ENGINE=Memory;
CREATE TEMPORARY TABLE tmp2 (n int) ENGINE=Log;
CREATE TEMPORARY TABLE tmp2 (n int) ORDER BY n; -- {serverError BAD_ARGUMENTS}
CREATE TEMPORARY TABLE tmp2 (n int, PRIMARY KEY (n)); -- {serverError BAD_ARGUMENTS}

CREATE TABLE log (n int);
SHOW CREATE log;
SET default_table_engine = 'MergeTree';
CREATE TABLE log1 AS log;
SHOW CREATE log1;
CREATE TABLE mem AS log1 ENGINE=Memory;
SHOW CREATE mem;
DROP TABLE log;
DROP TABLE log1;
DROP TABLE mem;

SET default_table_engine = 'None';
CREATE TABLE mem AS SELECT 1 as n; --{serverError ENGINE_REQUIRED}
SET default_table_engine = 'Memory';
CREATE TABLE mem ORDER BY n AS SELECT 1 as n; -- {serverError BAD_ARGUMENTS}
SET default_table_engine = 'MergeTree';
CREATE TABLE mt ORDER BY n AS SELECT 1 as n;
CREATE TABLE mem ENGINE=Memory AS SELECT 1 as n;
SHOW CREATE TABLE mt;
SHOW CREATE TABLE mem;
DROP TABLE mt;
DROP TABLE mem;

CREATE TABLE val AS values('n int', 1, 2);
CREATE TABLE val2 AS val;
CREATE TABLE log ENGINE=Log AS val;
SHOW CREATE TABLE val;
SHOW CREATE TABLE val2;
SHOW CREATE TABLE log;
DROP TABLE val;
DROP TABLE val2;
DROP TABLE log;

DROP TABLE IF EXISTS kek;
DROP TABLE IF EXISTS lol;
SET default_table_engine = 'Memory';
CREATE TABLE kek (n int) SETTINGS log_queries=1;
CREATE TABLE lol (n int) ENGINE=MergeTree ORDER BY n SETTINGS min_bytes_for_wide_part=123 SETTINGS log_queries=1;
SHOW CREATE TABLE kek;
SHOW CREATE TABLE lol;
DROP TABLE kek;
DROP TABLE lol;

SET default_temporary_table_engine = 'Log';
CREATE TEMPORARY TABLE tmp_log (n int);
SHOW CREATE TEMPORARY TABLE tmp_log;
