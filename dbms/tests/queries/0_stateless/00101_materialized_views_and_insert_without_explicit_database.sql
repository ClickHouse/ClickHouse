DROP TABLE IF EXISTS default.test_table;
DROP TABLE IF EXISTS default.test_view;

CREATE TABLE default.test_table (EventDate Date, CounterID UInt32,  UserID UInt64,  EventTime DateTime, UTCEventTime DateTime) ENGINE = Memory;
CREATE MATERIALIZED VIEW default.test_view (Rows UInt64,  MaxHitTime DateTime) ENGINE = Memory AS SELECT count() AS Rows, max(UTCEventTime) AS MaxHitTime FROM default.test_table;

INSERT INTO test_table (EventDate, UTCEventTime) VALUES ('2014-01-02', '2014-01-02 03:04:06');

SELECT * FROM default.test_table;
SELECT * FROM default.test_view;

DROP TABLE default.test_table;
DROP TABLE default.test_view;
