-- Tags: no-fasttest
-- Tag no-fasttest: the s3 table function is unavailable in fasttest builds.

-- The `body(...)` argument is supported only by the `url` table function.
-- The `s3` table function must reject it loudly instead of silently ignoring it.
-- The error is thrown during argument parsing, so no connection to the URL is made.
SELECT * FROM s3('http://localhost:11111/test/data.csv', body('payload')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:11111/test/data.csv', 'CSV', body((SELECT 1))); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:11111/test/data.csv', body('')); -- { serverError BAD_ARGUMENTS }
