-- Tags: no-fasttest
-- ^ no-fasttest: the s3 table function is gated on build flags

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104267:
-- a duplicated key-value argument in s3(...) is user input and must raise
-- BAD_ARGUMENTS, not LOGICAL_ERROR (which aborts debug builds and is treated as
-- a critical failure by stress/fuzzer checks). The duplicate is caught while
-- parsing arguments, before any network access, so a non-resolving host is fine.
--
-- NOTE: each query below throws on purpose, so the server logs its full query
-- text (this comment included) at Error level. Refer to the error only by its
-- code name LOGICAL_ERROR; never spell out the runtime message that
-- ci/jobs/scripts/log_parser.py greps for, or the scanner false-matches here.

SELECT * FROM s3('http://localhost:11111/foo', format = 'CSV', format = 'TSV'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:11111/foo', secret_access_key = 'a', secret_access_key = 'b'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:11111/foo', 'CSV', structure = 'a Int32', structure = 'b String'); -- { serverError BAD_ARGUMENTS }

SELECT 'ok';
