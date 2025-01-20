-- Tags: long, no-parallel
SET max_memory_usage = 6e8, max_rows_to_read = 300000000;
CREATE TABLE testing_memory (a UInt64, b String) engine=MergeTree ORDER BY ();
SET enable_memory_based_pipeline_throttling = 1;
INSERT INTO testing_memory SELECT number, toString(number) FROM system.numbers LIMIT 200000000 SETTINGS max_insert_threads=500;
SELECT count(*) FROM testing_memory;
SET enable_memory_based_pipeline_throttling = 0;
INSERT INTO testing_memory SELECT number, toString(number) FROM system.numbers LIMIT 200000000 SETTINGS max_insert_threads=500; -- { serverError MEMORY_LIMIT_EXCEEDED }
DROP TABLE testing_memory;
