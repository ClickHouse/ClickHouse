-- Test that clickhouse-local properly handles SYSTEM queries that are not supported
-- These queries should throw UNSUPPORTED_METHOD errors instead of LOGICAL_ERROR

-- Test SYSTEM RELOAD CONFIG in clickhouse-local
SYSTEM RELOAD CONFIG; -- { serverError UNSUPPORTED_METHOD }

-- Test SYSTEM STOP LISTEN in clickhouse-local
SYSTEM STOP LISTEN HTTP; -- { serverError UNSUPPORTED_METHOD }

-- Test SYSTEM START LISTEN in clickhouse-local
SYSTEM START LISTEN HTTP; -- { serverError UNSUPPORTED_METHOD }

-- Test with different server types
SYSTEM STOP LISTEN TCP; -- { serverError UNSUPPORTED_METHOD }
SYSTEM START LISTEN TCP; -- { serverError UNSUPPORTED_METHOD }

-- Test that other SYSTEM queries that should work in clickhouse-local still work
SYSTEM DROP DNS CACHE;
SYSTEM DROP MARK CACHE;
SYSTEM DROP UNCOMPRESSED CACHE;
SYSTEM DROP QUERY CACHE;
SYSTEM DROP SCHEMA CACHE;
SYSTEM DROP FORMAT SCHEMA CACHE;

-- Test that queries with proper error messages are thrown
-- (These should work in server mode but not in clickhouse-local)
SYSTEM RELOAD CONFIG; -- { serverError UNSUPPORTED_METHOD }
SYSTEM STOP LISTEN HTTP; -- { serverError UNSUPPORTED_METHOD }
SYSTEM START LISTEN HTTP; -- { serverError UNSUPPORTED_METHOD } 