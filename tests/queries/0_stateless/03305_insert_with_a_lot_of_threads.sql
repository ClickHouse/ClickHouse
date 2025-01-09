-- will be a long test later, now I don't mark it as long because I want to test it in fast tests
SET max_memory_usage = 2e9, max_rows_to_read = 0, max_execution_time = 200;
CREATE TABLE testing_memory (a UInt64, b String) engine=MergeTree ORDER BY ();
INSERT INTO testing_memory SELECT number, toString(number) FROM system.numbers LIMIT 1000000000 SETTINGS max_insert_threads=500;
SELECT count(*) FROM testing_memory;
DROP TABLE testing_memory;
