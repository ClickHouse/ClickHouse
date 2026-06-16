-- Tags: no-fasttest
-- no-fasttest: requires S3 storage

-- A `partition_strategy` passed positionally must be recognized regardless of case; otherwise a
-- lowercase strategy is misrouted to `compression_method`. All three must fail with BAD_ARGUMENTS
-- because no PARTITION BY is given for a read.
SELECT * FROM s3('http://localhost:11111/test/test', 'key', 'secret', 'token', 'Parquet', 'col1 UInt32', 'HIVE') LIMIT 0; -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:11111/test/test', 'key', 'secret', 'token', 'Parquet', 'col1 UInt32', 'hive') LIMIT 0; -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:11111/test/test', 'key', 'secret', 'token', 'Parquet', 'col1 UInt32', 'Hive') LIMIT 0; -- { serverError BAD_ARGUMENTS }

-- 8-arg form: a bool last argument means the 7th is `partition_strategy`, so `(..., 'NONE', 1)` must parse.
SELECT * FROM s3('http://localhost:11111/test/test', 'key', 'secret', 'token', 'Parquet', 'col1 UInt32', 'NONE', 1) LIMIT 0;

-- 7-arg: uppercase `NONE` is the positional partition strategy, not compression (read failed before the fix).
INSERT INTO FUNCTION s3('http://localhost:11111/test/04337_none7_' || currentDatabase() || '.csv', 'test', 'testtest', '', 'CSV', 'a UInt32') SELECT 7 SETTINGS s3_truncate_on_insert = 1;
SELECT * FROM s3('http://localhost:11111/test/04337_none7_' || currentDatabase() || '.csv', 'test', 'testtest', '', 'CSV', 'a UInt32', 'NONE');
