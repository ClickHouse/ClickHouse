-- Tags: no-parallel, no-fasttest
-- Tag no-fasttest: Depends on OpenSSL
-- Tag no-parallel: Messes with internal cache

SET allow_experimental_query_cache = true;

-- The test assumes that these two settings have default values. Neutralize the effect of setting randomization:
SET use_query_cache = false;
SET enable_reads_from_query_cache = true;

SYSTEM DROP QUERY CACHE;

-- Cache a result of a query with secret in the query cache
SELECT hex(encrypt('aes-128-ecb', 'plaintext', 'passwordpassword')) SETTINGS use_query_cache = true;

-- The secret should not be revealed in system.query_cache
SELECT query FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
