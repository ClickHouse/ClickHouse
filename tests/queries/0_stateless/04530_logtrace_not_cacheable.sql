-- `logTrace` has an observable execution-time side effect (a trace message per block), so it is not
-- deterministic. The query result cache must therefore refuse to cache a query containing it under the
-- default `query_cache_nondeterministic_function_handling = 'throw'`. Otherwise the query could be
-- cached once and a later cache hit would return the constant `0` result while silently dropping the
-- per-block logging that `logTrace` is documented to perform.
SELECT logTrace('test_04530') FROM numbers(3) SETTINGS max_block_size = 1, use_query_cache = 1 FORMAT Null; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
