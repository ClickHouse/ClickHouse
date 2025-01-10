-- Tags: long
SET max_memory_usage = 9e8, max_rows_to_read = 0;
CREATE TABLE testing_memory (a UInt64, b String) engine=MergeTree ORDER BY ();
INSERT INTO testing_memory SELECT number, toString(number) FROM system.numbers LIMIT 200000000 SETTINGS max_insert_threads=500;
SELECT count(*) FROM testing_memory;
DROP TABLE testing_memory;
