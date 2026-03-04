-- Tags: no-async-insert
-- The correct profile event appears in the secondary query with query_kind: AsyncInsertFlush
-- add_minmax_index_for_numeric_columns=0: We are checking exact rows read, and that number will be different if we have an index on v

DROP TABLE IF EXISTS rows_events_test;
CREATE TABLE rows_events_test (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k SETTINGS add_minmax_index_for_numeric_columns=0;

INSERT INTO /* test 01413, query 1 */ rows_events_test VALUES (1,1);
SYSTEM FLUSH LOGS query_log;

SELECT written_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE 'INSERT INTO /* test 01413, query 1 */ rows_events_test%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

SELECT ProfileEvents['InsertedRows'] as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE 'INSERT INTO /* test 01413, query 1 */ rows_events_test%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;


INSERT INTO /* test 01413, query 2 */ rows_events_test VALUES (2,2), (3,3);
SYSTEM FLUSH LOGS query_log;

SELECT written_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE 'INSERT INTO /* test 01413, query 2 */ rows_events_test%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;

SELECT ProfileEvents['InsertedRows'] as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE 'INSERT INTO /* test 01413, query 2 */ rows_events_test%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;


SELECT * FROM /* test 01413, query 3 */ rows_events_test WHERE v = 2;
SYSTEM FLUSH LOGS query_log;

SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE 'SELECT * FROM /* test 01413, query 3 */ rows_events_test%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;


SELECT ProfileEvents['SelectedRows'] as value FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE 'SELECT * FROM /* test 01413, query 3 */ rows_events_test%' AND type = 2 AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 1;


DROP TABLE rows_events_test;
