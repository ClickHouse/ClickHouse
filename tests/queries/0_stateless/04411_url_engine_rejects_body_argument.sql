-- The `body(...)` argument is supported only by the `url` table function, where it forms the HTTP
-- request body of a `SELECT`. The persistent `URL` table engine has no place for it (its `INSERT`
-- already streams the inserted rows as the body, and the engine syntax is documented as
-- `URL(URL [,Format] [,CompressionMethod])`). It must be rejected loudly during `CREATE`, instead of
-- being silently accepted and later issuing an undocumented `POST` on `SELECT`.
-- The error is thrown while parsing the engine arguments, so no connection to the URL is made.

DROP TABLE IF EXISTS t_url_engine_body;

CREATE TABLE t_url_engine_body (x UInt8) ENGINE = URL('http://localhost:11111/test/data', JSONEachRow, body('payload')); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_url_engine_body (x UInt8) ENGINE = URL('http://localhost:11111/test/data', JSONEachRow, body('')); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_url_engine_body;
