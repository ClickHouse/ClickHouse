-- Test that SYSTEM queries work properly in server mode
-- These queries should work in server mode but be restricted in clickhouse-local

-- Test SYSTEM RELOAD CONFIG in server mode (should work)
SYSTEM RELOAD CONFIG;

-- Test SYSTEM STOP LISTEN in server mode (should work)
SYSTEM STOP LISTEN HTTP;

-- Test SYSTEM START LISTEN in server mode (should work)
SYSTEM START LISTEN HTTP;

-- Test with different server types
SYSTEM STOP LISTEN TCP;
SYSTEM START LISTEN TCP;

-- Test that other SYSTEM queries continue to work
SYSTEM DROP DNS CACHE;
SYSTEM DROP MARK CACHE;
SYSTEM DROP UNCOMPRESSED CACHE;
SYSTEM DROP QUERY CACHE;
SYSTEM DROP SCHEMA CACHE;
SYSTEM DROP FORMAT SCHEMA CACHE;

-- Test that the queries work again after stopping/starting
SYSTEM STOP LISTEN HTTP;
SYSTEM START LISTEN HTTP; 