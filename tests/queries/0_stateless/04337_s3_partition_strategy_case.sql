-- Tags: no-fasttest
-- no-fasttest: requires S3 storage

-- A `partition_strategy` passed positionally must be recognized regardless of case; otherwise a
-- lowercase strategy is misrouted to `compression_method`. All three must fail with BAD_ARGUMENTS
-- because no PARTITION BY is given for a read.
SELECT * FROM s3('http://localhost:11111/test/test', 'key', 'secret', 'token', 'Parquet', 'col1 UInt32', 'HIVE') LIMIT 0; -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:11111/test/test', 'key', 'secret', 'token', 'Parquet', 'col1 UInt32', 'hive') LIMIT 0; -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:11111/test/test', 'key', 'secret', 'token', 'Parquet', 'col1 UInt32', 'Hive') LIMIT 0; -- { serverError BAD_ARGUMENTS }

-- `none` must stay a `compression_method`, not be captured as `partition_strategy`. Here the 6th arg
-- is the compression method, so the 7th is the strategy: an unknown strategy must fail with BAD_ARGUMENTS.
SELECT * FROM s3('http://localhost:11111/test/test', 'key', 'secret', 'token', 'Parquet', 'col1 UInt32', 'none', 'unknown_strategy') LIMIT 0; -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:11111/test/test', 'key', 'secret', 'token', 'Parquet', 'col1 UInt32', 'NONE', 'unknown_strategy') LIMIT 0; -- { serverError BAD_ARGUMENTS }
