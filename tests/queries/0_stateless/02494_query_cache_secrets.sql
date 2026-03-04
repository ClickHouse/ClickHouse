-- Tags: no-parallel, no-fasttest
-- Tag no-fasttest: Depends on OpenSSL
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- Cache a result of a query with secret in the query cache
SELECT hex(encrypt('aes-128-ecb', 'plaintext', 'passwordpassword')) SETTINGS use_query_cache = true;

-- The secret should not be revealed in system.query_cache
SELECT query FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
