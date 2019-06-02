CREATE TABLE test_table (EventDate Date, CounterID UInt32,  UserID UInt64,  EventTime DateTime, UTCEventTime DateTime) ENGINE = MergeTree(EventDate, CounterID, 8192);
CREATE MATERIALIZED VIEW test_view (Rows UInt64,  MaxHitTime DateTime) ENGINE = Memory AS SELECT count() AS Rows, max(UTCEventTime) AS MaxHitTime FROM test_table;
CREATE MATERIALIZED VIEW test_view_filtered (EventDate Date, CounterID UInt32) ENGINE = Memory POPULATE AS SELECT CounterID, EventDate FROM test_table WHERE EventDate < '2013-01-01';

INSERT INTO test_table (EventDate, UTCEventTime) VALUES ('2014-01-02', '2014-01-02 03:04:06');

SELECT * FROM test_table;
SELECT * FROM test_view;
SELECT * FROM test_view_filtered;

-- Check only sophisticated constructors and desctructors:

CREATE TABLE tmp (date Date, name String) ENGINE = Memory;
CREATE MATERIALIZED VIEW tmp_mv ENGINE = AggregatingMergeTree(date, (date, name), 8192) AS SELECT date, name, countState() AS cc FROM tmp GROUP BY date, name;
CREATE TABLE tmp_mv2 AS tmp_mv;
CREATE TABLE tmp_mv3 AS tmp_mv ENGINE = Memory;
CREATE MATERIALIZED VIEW tmp_mv4 ENGINE = AggregatingMergeTree(date, date, 8192) POPULATE AS SELECT DISTINCT * FROM tmp_mv;

EXISTS TABLE `.inner.tmp_mv`;
EXISTS TABLE `.inner.tmp_mv2`;
EXISTS TABLE `.inner.tmp_mv3`;
EXISTS TABLE `.inner.tmp_mv4`;
