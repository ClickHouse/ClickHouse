-- getClientHTTPHeader returns a per-request value, so it must be treated as non-deterministic and
-- therefore rejected by the query result cache (which throws by default for non-deterministic functions).
SET allow_get_client_http_header = 1;
SELECT getClientHTTPHeader('X-Test-04404') SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
