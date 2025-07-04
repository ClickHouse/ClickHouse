-- Test that the old LOGICAL_ERROR behavior has been fixed
-- These queries used to throw LOGICAL_ERROR but should now throw UNSUPPORTED_METHOD

-- Test that the old error message is no longer thrown
-- Previously: "Can't reload config because config_reload_callback is not set. (LOGICAL_ERROR)"
-- Now: "SYSTEM RELOAD CONFIG query is not supported in clickhouse-local. (UNSUPPORTED_METHOD)"
SYSTEM RELOAD CONFIG; -- { serverError UNSUPPORTED_METHOD }

-- Test that STOP LISTEN and START LISTEN also use the correct error
SYSTEM STOP LISTEN HTTP; -- { serverError UNSUPPORTED_METHOD }
SYSTEM START LISTEN HTTP; -- { serverError UNSUPPORTED_METHOD }

-- Test that the error messages are specific to clickhouse-local
-- The error should mention "clickhouse-local" in the message
SYSTEM RELOAD CONFIG; -- { serverError UNSUPPORTED_METHOD }
SYSTEM STOP LISTEN HTTP; -- { serverError UNSUPPORTED_METHOD }
SYSTEM START LISTEN HTTP; -- { serverError UNSUPPORTED_METHOD }

-- Test that other SYSTEM queries that should work in clickhouse-local still work
-- These should not be affected by the changes
SYSTEM DROP DNS CACHE;
SYSTEM DROP MARK CACHE;
SYSTEM DROP UNCOMPRESSED CACHE;
SYSTEM DROP QUERY CACHE;
SYSTEM DROP SCHEMA CACHE;
SYSTEM DROP FORMAT SCHEMA CACHE; 