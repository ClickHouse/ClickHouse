-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

SET query_cache_tag = '02494_query_cache_secrets';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_secrets';

-- Cache a result of a query with secret in the query cache
SELECT hex(encrypt('aes-128-ecb', 'plaintext', 'passwordpassword')) SETTINGS use_query_cache = true;

-- The secret should not be revealed in system.query_cache
SELECT query FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_secrets') AS test_query_cache;

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_secrets';

-- https://github.com/ClickHouse/ClickHouse/issues/102927
-- HMAC key should be hidden in system.query_cache
SELECT hex(HMAC('sha256', 'message', 'this_should_be_secret')) SETTINGS use_query_cache = true;

-- The HMAC key should not be revealed in system.query_cache
SELECT query FROM (SELECT * FROM system.query_cache WHERE tag = '02494_query_cache_secrets') AS test_query_cache;

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_secrets';
