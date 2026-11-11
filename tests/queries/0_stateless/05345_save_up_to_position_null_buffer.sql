-- Tags: no-fasttest
-- no-fasttest: uses the S3 mock server on localhost:11111

-- `ReaderExecutor` nulls the read position at EOF, and `saveUpToPosition` passed it to `memcpy`.
INSERT INTO FUNCTION s3('http://localhost:11111/test/05345_' || currentDatabase() || '.tsv', 'test', 'testtest', 'TSV', 'x UInt64')
    SELECT number FROM numbers(3) SETTINGS s3_truncate_on_insert = 1;

SELECT sum(x) FROM s3('http://localhost:11111/test/05345_' || currentDatabase() || '.tsv', 'test', 'testtest', 'TSV', 'x UInt64')
SETTINGS use_reader_executor = 1, remote_filesystem_read_method = 'read', input_format_parallel_parsing = 1, max_parsing_threads = 2;
