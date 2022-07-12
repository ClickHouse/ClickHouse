DROP TABLE IF EXISTS lwd_test;

CREATE TABLE lwd_test (id UInt64 , value String) ENGINE MergeTree() ORDER BY id;

INSERT INTO lwd_test SELECT number, randomString(10) FROM system.numbers LIMIT 10000000;

SET mutations_sync = 1;
--SET allow_experimental_lightweight_delete = 1;

SELECT 'Rows in parts', SUM(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'lwd_test' AND active;

SELECT 'Count', count() FROM lwd_test WHERE id >= 0;

SELECT 'First row', id, length(value) FROM lwd_test ORDER BY id LIMIT 1;


SELECT 'Delete 3M rows using light weight delete';
ALTER TABLE lwd_test UPDATE __row_exists = 0 WHERE id < 3000000;

SELECT 'Rows in parts', SUM(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'lwd_test' AND active;

SELECT 'Count', count() FROM lwd_test WHERE id >= 0;

SELECT 'First row', id, length(value) FROM lwd_test ORDER BY id LIMIT 1;


SELECT 'Force merge to cleanup deleted rows';
OPTIMIZE TABLE lwd_test FINAL;

SELECT 'Rows in parts', SUM(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'lwd_test' AND active;

SELECT 'Count', count() FROM lwd_test WHERE id >= 0;

SELECT 'First row', id, length(value) FROM lwd_test ORDER BY id LIMIT 1;


DROP TABLE lwd_test;