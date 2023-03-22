-- Tags: no-parallel, no-fasttest
-- Tag no-fasttest: Depends on OpenSSL
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY RESULT CACHE;

-- Cache a result of a query with secret in the query result cache
SELECT hex(encrypt('aes-128-ecb', 'plaintext', 'passwordpassword')) SETTINGS enable_experimental_query_result_cache = true;

-- The secret should not be revealed in system.query_result_cache
SELECT query FROM system.query_result_cache;

SYSTEM DROP QUERY RESULT CACHE;
