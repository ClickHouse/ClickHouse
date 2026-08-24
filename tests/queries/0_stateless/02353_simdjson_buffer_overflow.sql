-- Tag: no-msan: fuzzer can make this query very memory hungry, and under MSan, the MemoryTracker cannot account for the additional memory used by sanitizer, and OOM happens.

SET max_execution_time = 3;
SET timeout_overflow_mode = 'break';
SET max_rows_to_read = 0, max_bytes_to_read = 0;

SELECT count() FROM system.numbers_mt WHERE NOT ignore(JSONExtract('{' || repeat('"a":"b",', rand() % 10) || '"c":"d"}', 'a', 'String')) FORMAT Null;
