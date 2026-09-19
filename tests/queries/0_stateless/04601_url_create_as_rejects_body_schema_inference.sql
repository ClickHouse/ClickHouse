-- `CREATE TABLE ... AS url(..., body(...))` is an insert-style use of the table function and is
-- rejected, because inserting into `url` sends the inserted rows as the request body. With the
-- columns omitted, the rejection must fire before the schema-inference request: schema inference
-- would otherwise deliver the body-carrying `POST` to the endpoint even though the query is doomed
-- to fail. `BAD_ARGUMENTS` is thrown before any connection is attempted, so the unroutable address
-- below is never contacted (see also the request-counting integration test in
-- `test_storage_url_http_body`).

DROP TABLE IF EXISTS t_url_create_as_body;

CREATE TABLE t_url_create_as_body AS url('http://localhost:11111/test/data', 'JSONEachRow', body('payload')); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_url_create_as_body AS url('http://localhost:11111/test/data', 'JSONEachRow', body((SELECT 1))); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_url_create_as_body AS url('http://localhost:11111/test/data', 'JSONEachRow', body('')); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_url_create_as_body AS urlCluster('test_shard_localhost', 'http://localhost:11111/test/data', 'JSONEachRow', body('payload')); -- { serverError BAD_ARGUMENTS }

-- With an explicit structure there is no schema-inference request; the rejection then comes from
-- the storage-creation guard, still before any connection.
CREATE TABLE t_url_create_as_body AS url('http://localhost:11111/test/data', 'JSONEachRow', 'x UInt8', body('payload')); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_url_create_as_body;
