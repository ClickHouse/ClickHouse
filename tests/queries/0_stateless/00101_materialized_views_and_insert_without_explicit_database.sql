CREATE DATABASE IF NOT EXISTS test_00101_0;

USE test_00101_0;

DROP TABLE IF EXISTS test_table;
DROP TABLE IF EXISTS test_view;
DROP TABLE IF EXISTS test_view_filtered;

CREATE TABLE test_table (EventDate Date, CounterID UInt32,  UserID UInt64,  EventTime DateTime('Europe/Moscow'), UTCEventTime DateTime('UTC')) ENGINE = MergeTree(EventDate, CounterID, 8192);
CREATE MATERIALIZED VIEW test_view (Rows UInt64,  MaxHitTime DateTime('Europe/Moscow')) ENGINE = Memory AS SELECT count() AS Rows, max(UTCEventTime) AS MaxHitTime FROM test_table;
CREATE MATERIALIZED VIEW test_view_filtered (EventDate Date, CounterID UInt32) ENGINE = Memory POPULATE AS SELECT CounterID, EventDate FROM test_table WHERE EventDate < '2013-01-01';

INSERT INTO test_table (EventDate, UTCEventTime) VALUES ('2014-01-02', '2014-01-02 03:04:06');

SELECT * FROM test_table;
SELECT * FROM test_view;
SELECT * FROM test_view_filtered;

DROP TABLE test_table;
DROP TABLE test_view;
DROP TABLE test_view_filtered;

-- Check only sophisticated constructors and desctructors:

CREATE DATABASE IF NOT EXISTS test_00101_1;

USE test_00101_1;

DROP TABLE IF EXISTS tmp;
DROP TABLE IF EXISTS tmp_mv;
DROP TABLE IF EXISTS tmp_mv2;
DROP TABLE IF EXISTS tmp_mv3;
DROP TABLE IF EXISTS tmp_mv4;
DROP TABLE IF EXISTS `.inner.tmp_mv`;
DROP TABLE IF EXISTS `.inner.tmp_mv2`;
DROP TABLE IF EXISTS `.inner.tmp_mv3`;
DROP TABLE IF EXISTS `.inner.tmp_mv4`;

CREATE TABLE tmp (date Date, name String) ENGINE = Memory;
CREATE MATERIALIZED VIEW tmp_mv ENGINE = AggregatingMergeTree(date, (date, name), 8192) AS SELECT date, name, countState() AS cc FROM tmp GROUP BY date, name;
CREATE TABLE tmp_mv2 AS tmp_mv;
CREATE TABLE tmp_mv3 AS tmp_mv ENGINE = Memory;
CREATE MATERIALIZED VIEW tmp_mv4 ENGINE = AggregatingMergeTree(date, date, 8192) POPULATE AS SELECT DISTINCT * FROM tmp_mv;

DROP TABLE tmp_mv;
DROP TABLE tmp_mv2;
DROP TABLE tmp_mv3;
DROP TABLE tmp_mv4;

EXISTS TABLE `.inner.tmp_mv`;
EXISTS TABLE `.inner.tmp_mv2`;
EXISTS TABLE `.inner.tmp_mv3`;
EXISTS TABLE `.inner.tmp_mv4`;

DROP TABLE tmp;

DROP DATABASE test_00101_0;
DROP DATABASE test_00101_1;
