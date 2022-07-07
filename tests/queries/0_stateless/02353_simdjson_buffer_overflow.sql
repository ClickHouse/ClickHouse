-- Tags: no-fasttest

SET max_execution_time = 3;
SET timeout_overflow_mode = 'break';

SELECT count() FROM system.numbers_mt WHERE NOT ignore(JSONExtract('{' || repeat('"a":"b",', rand() % 10) || '"c":"d"}', 'a', 'String')) FORMAT Null;
