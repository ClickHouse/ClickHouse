DROP TABLE IF EXISTS test_lwd;

CREATE TABLE test_lwd (id UInt64)
ENGINE MergeTree() ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, index_granularity = 8192;

SET lightweight_delete_mode = 'lightweight_update_force';

INSERT INTO test_lwd SELECT number FROM numbers(100000);

SET max_rows_to_read = 10000;
SET force_primary_key = 1;

DELETE FROM test_lwd WHERE id IN (10000, 10001, 10002);
DELETE FROM test_lwd WHERE id IN (SELECT number FROM numbers(10003, 3));

SELECT count() FROM test_lwd WHERE id IN (SELECT number FROM numbers(10000, 3));
SELECT count() FROM test_lwd WHERE id IN (SELECT number FROM numbers(10003, 3));
SELECT count() FROM test_lwd WHERE id IN (SELECT number FROM numbers(10006, 3));

DROP TABLE IF EXISTS test_lwd;
