-- Verify that getClientHTTPHeader is treated as non-deterministic and
-- cannot be used together with the query cache.
SYSTEM DROP QUERY CACHE;
SELECT getClientHTTPHeader('X-Test') SETTINGS allow_get_client_http_header = 1, use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SYSTEM DROP QUERY CACHE;
