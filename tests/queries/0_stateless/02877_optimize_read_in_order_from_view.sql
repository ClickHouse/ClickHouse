SET optimize_read_in_order=1;

DROP TABLE IF EXISTS view1;
DROP TABLE IF EXISTS table1;

CREATE TABLE table1 (number UInt64) ENGINE=MergeTree ORDER BY number SETTINGS index_granularity=1;
INSERT INTO table1 SELECT number FROM numbers(1, 300);

CREATE VIEW view1 AS SELECT number FROM table1;

-- The following SELECT is expected to read 20 rows. In fact it may decide to read more than 20 rows, but not too many anyway.
-- So we'll check that the number of read rows is less than 40.

SELECT /* test 02877, query 1 */ * FROM (SELECT * FROM view1) ORDER BY number DESC LIMIT 20 SETTINGS log_queries=1;

SYSTEM FLUSH LOGS;

SELECT concat('read_rows=', if(read_rows<40, 'ok', toString(read_rows))) FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE '%test 02877, query 1%' AND type='QueryFinish';

DROP TABLE view1;
DROP TABLE table1;
