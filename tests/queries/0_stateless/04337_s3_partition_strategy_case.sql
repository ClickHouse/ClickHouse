-- Tags: no-fasttest
-- no-fasttest: requires S3 storage

-- A `partition_strategy` passed positionally must be recognized regardless of case; otherwise a
-- lowercase strategy is misrouted to `compression_method`. All three must fail with BAD_ARGUMENTS
-- because no PARTITION BY is given for a read.
SELECT * FROM s3('http://localhost:11111/test/test', 'key', 'secret', 'token', 'Parquet', 'col1 UInt32', 'HIVE') LIMIT 0; -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:11111/test/test', 'key', 'secret', 'token', 'Parquet', 'col1 UInt32', 'hive') LIMIT 0; -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:11111/test/test', 'key', 'secret', 'token', 'Parquet', 'col1 UInt32', 'Hive') LIMIT 0; -- { serverError BAD_ARGUMENTS }
