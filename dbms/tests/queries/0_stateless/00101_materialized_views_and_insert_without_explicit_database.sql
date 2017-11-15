DROP TABLE IF EXISTS default.test_table;
DROP TABLE IF EXISTS default.test_view;

CREATE TABLE default.test_table (EventDate Date, CounterID UInt32,  UserID UInt64,  EventTime DateTime, UTCEventTime DateTime) ENGINE = Memory;
CREATE MATERIALIZED VIEW default.test_view (Rows UInt64,  MaxHitTime DateTime) ENGINE = Memory AS SELECT count() AS Rows, max(UTCEventTime) AS MaxHitTime FROM default.test_table;

INSERT INTO test_table (EventDate, UTCEventTime) VALUES ('2014-01-02', '2014-01-02 03:04:06');

SELECT * FROM default.test_table;
SELECT * FROM default.test_view;

DROP TABLE default.test_table;
DROP TABLE default.test_view;

-- Check only sophisticated constructors and desctructors:

USE test;
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
