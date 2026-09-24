-- Tags: no-fasttest
-- no-fasttest: needs the `s3` table function

-- Exceptions thrown while validating an S3 URI must not reveal the credentials embedded in it:
-- the exception text is stored in `system.query_log`, which, unlike the query text, is not masked.

SET log_queries = 1;

-- Bucket name is too short.
SELECT count() FROM s3('http://s3user:SECRET_bucket@127.0.0.1:9/b/k.csv', 'CSV'); -- { serverError BAD_ARGUMENTS }
-- Key with consecutive slashes.
SELECT count() FROM s3('http://ku:SECRET_key@my-bucket-name.s3.amazonaws.com//a//b.csv', 'CSV'); -- { serverError BAD_ARGUMENTS }
-- Not a virtual-hosted-style URI.
SELECT count() FROM s3('http://vu:SECRET_virtual@127.0.0.1:9/bucketname/k.csv', 'CSV') SETTINGS s3_uri_style = 'virtual_hosted'; -- { serverError BAD_ARGUMENTS }
-- Not a path-style URI.
SELECT count() FROM s3('http://pu:SECRET_path@127.0.0.1:9', 'CSV') SETTINGS s3_uri_style = 'path'; -- { serverError BAD_ARGUMENTS }
-- The query parameters of a presigned URL.
SELECT count() FROM s3('http://127.0.0.1:9/b/k.csv?X-Amz-Signature=SECRET_signature', 'CSV'); -- { serverError BAD_ARGUMENTS }
-- The same through a table engine.
CREATE TABLE s3_engine (x UInt8) ENGINE = S3('http://duser:SECRET_engine@127.0.0.1:9/b/x.csv', 'CSV'); -- { serverError BAD_ARGUMENTS }

SYSTEM FLUSH LOGS query_log;

SELECT type, position(exception, 'SECRET_') > 0 AS leaked, extract(exception, 'DB::Exception: (.*)\\. \\(BAD_ARGUMENTS\\)') AS message
FROM system.query_log
WHERE current_database = currentDatabase() AND type != 'QueryStart' AND exception_code = 36
ORDER BY event_time_microseconds;
