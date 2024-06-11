DROP TABLE IF EXISTS queue_mode_test;

CREATE TABLE queue_mode_test(a UInt64) ENGINE=MergeTree ORDER BY a SETTINGS queue_mode=1, allow_experimental_block_number_column=1;

SET async_insert = 1;
SET wait_for_async_insert = 0;
SET async_insert_busy_timeout_min_ms = 100000;
SET async_insert_busy_timeout_max_ms = 1000000;
SET asterisk_include_materialized_columns = 1;

INSERT INTO queue_mode_test VALUES (1);
INSERT INTO queue_mode_test VALUES (2);
INSERT INTO queue_mode_test VALUES (3);
INSERT INTO queue_mode_test VALUES (4);

SYSTEM FLUSH ASYNC INSERT QUEUE;
OPTIMIZE TABLE queue_mode_test;

SELECT a, _queue_block_number == _block_number FROM queue_mode_test ORDER BY a;
