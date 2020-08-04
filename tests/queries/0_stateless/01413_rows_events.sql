DROP TABLE IF EXISTS rows_events_test;
CREATE TABLE rows_events_test (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;

SYSTEM FLUSH LOGS;
TRUNCATE TABLE system.query_log;
INSERT INTO rows_events_test VALUES (1,1);
SYSTEM FLUSH LOGS;
SELECT written_rows FROM system.query_log WHERE query LIKE 'INSERT INTO rows_events_test%' AND type=2;
SELECT value FROM (
    SELECT ProfileEvents.Names as name, ProfileEvents.Values as value FROM system.query_log ARRAY JOIN ProfileEvents WHERE query LIKE 'INSERT INTO rows_events_test%' AND type=2
) WHERE name='InsertedRows';

SYSTEM FLUSH LOGS;
TRUNCATE TABLE system.query_log;
INSERT INTO rows_events_test VALUES (2,2), (3,3);
SYSTEM FLUSH LOGS;
SELECT written_rows FROM system.query_log WHERE query LIKE 'INSERT INTO rows_events_test%' AND type=2;
SELECT value FROM (
    SELECT ProfileEvents.Names as name, ProfileEvents.Values as value FROM system.query_log ARRAY JOIN ProfileEvents WHERE query LIKE 'INSERT INTO rows_events_test%' AND type=2
) WHERE name='InsertedRows';

SYSTEM FLUSH LOGS;
TRUNCATE TABLE system.query_log;
SELECT * FROM rows_events_test WHERE v = 2;
SYSTEM FLUSH LOGS;
SELECT read_rows FROM system.query_log WHERE query LIKE 'SELECT * FROM rows_events_test%' AND type=2;
SELECT value FROM (
    SELECT ProfileEvents.Names as name, ProfileEvents.Values as value FROM system.query_log ARRAY JOIN ProfileEvents WHERE query LIKE 'SELECT * FROM rows_events_test%' AND type=2
) WHERE name='SelectedRows';

DROP TABLE rows_events_test;
