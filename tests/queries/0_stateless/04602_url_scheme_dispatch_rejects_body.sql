-- The `url` table function dispatches by URL scheme (`file://`, `s3://`, `az://`, `hdfs://`, ...)
-- to another table function. The rebuilt delegate argument list carries only the source, format,
-- structure and compression method, and the delegate backends read without an HTTP request body
-- anyway, so a `body(...)` argument would be silently dropped. It must be rejected loudly,
-- consistently with the `headers(...)` rejection on the same path. The rejection fires while
-- parsing the arguments, before the delegate is even built, so it does not depend on the backend
-- being available and no data source is contacted.

SELECT * FROM url('file://nonexistent.csv', 'CSV', 'a UInt32', body('payload')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('file://nonexistent.csv', 'CSV', 'a UInt32', body((SELECT 1))); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('file://nonexistent.csv', 'CSV', 'a UInt32', body('')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('s3://bucket/nonexistent', 'CSV', 'a UInt32', body('payload')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('gs://bucket/nonexistent', 'CSV', 'a UInt32', body('payload')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('hdfs://namenode:9000/nonexistent', 'CSV', 'a UInt32', body('payload')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url('az://account/container/nonexistent', 'CSV', 'a UInt32', body('payload')); -- { serverError BAD_ARGUMENTS }
