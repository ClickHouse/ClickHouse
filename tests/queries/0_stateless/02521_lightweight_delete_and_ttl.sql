DROP TABLE IF EXISTS lwd_test_02521;

CREATE TABLE lwd_test_02521 (id UInt64, value String, event_time DateTime)
ENGINE MergeTree()
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO lwd_test_02521 SELECT number, randomString(10), now() - INTERVAL 2 MONTH FROM numbers(50000);
INSERT INTO lwd_test_02521 SELECT number, randomString(10), now() FROM numbers(50000);

OPTIMIZE TABLE lwd_test_02521 FINAL SETTINGS mutations_sync = 1;

SET mutations_sync=1;

-- { echoOn }
SELECT 'Rows in parts', SUM(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'lwd_test_02521' AND active;
SELECT 'Count', count() FROM lwd_test_02521;


DELETE FROM lwd_test_02521 WHERE id < 25000;

SELECT 'Rows in parts', SUM(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'lwd_test_02521' AND active;
SELECT 'Count', count() FROM lwd_test_02521;


ALTER TABLE lwd_test_02521 MODIFY TTL event_time + INTERVAL 1 MONTH SETTINGS mutations_sync = 1;

SELECT 'Rows in parts', SUM(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'lwd_test_02521' AND active;
SELECT 'Count', count() FROM lwd_test_02521;


ALTER TABLE lwd_test_02521 DELETE WHERE id >= 40000 SETTINGS mutations_sync = 1;

SELECT 'Rows in parts', SUM(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'lwd_test_02521' AND active;
SELECT 'Count', count() FROM lwd_test_02521;


OPTIMIZE TABLE lwd_test_02521 FINAL SETTINGS mutations_sync = 1;

SELECT 'Rows in parts', SUM(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'lwd_test_02521' AND active;
SELECT 'Count', count() FROM lwd_test_02521;

-- { echoOff }

DROP TABLE lwd_test_02521;
