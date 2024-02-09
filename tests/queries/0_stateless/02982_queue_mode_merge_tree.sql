set asterisk_include_materialized_columns=1;

DROP TABLE IF EXISTS queue_mode_test;

CREATE TABLE queue_mode_test(a UInt64, b UInt64) ENGINE=MergeTree() ORDER BY (a) SETTINGS queue=1;

SELECT 'start';
SELECT * FROM queue_mode_test;

SELECT 'insert some data';
INSERT INTO queue_mode_test (*) SELECT number, number FROM numbers(2);
INSERT INTO queue_mode_test (*) SELECT number, number FROM numbers(3);
INSERT INTO queue_mode_test (*) SELECT number, number FROM numbers(4);
INSERT INTO queue_mode_test (*) SELECT number, number FROM numbers(5);

SELECT 'optimize table to create single part';
OPTIMIZE TABLE queue_mode_test;

SELECT * FROM queue_mode_test;

SELECT 'cursor lookup';
SELECT * FROM queue_mode_test WHERE (_queue_block_number, _queue_block_offset) > (3, 1);

SELECT 'end';
