-- Tags: no-fasttest
-- no-fasttest: s3/gcs table functions are not available in the fast test build.

-- A query that mixes the positional secret form `s3('url', 'id', 'key')` with the
-- named secret form `secret_access_key = '...'` used to abort the server: the secret
-- arguments finder marked the named argument first (a higher index), then asked to mark
-- the earlier positional one, tripping a chassert(index >= result.start) while masking.
-- Masking runs over arbitrary user input during error formatting, so it must not crash.
-- These queries are malformed and must produce a normal error, not abort the server.

SELECT * FROM s3('url', 'a', 'b', secret_access_key = 'c'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM gcs('url', 'a', 'b', secret_access_key = 'c'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM s3('http://localhost:11111/foo', equals(secret_access_key, materialize(257), 'a'), toString(NULL), secret_access_key = 'b'); -- { serverError BAD_ARGUMENTS }

-- The server is still alive and well-formed masked queries are unaffected.
SELECT 1;
