-- Tags: no-parallel
-- Tag no-parallel: issues `SYSTEM DROP QUERY CACHE`, which the style check requires to run serially.

-- `ASTSetQuery` carries three payloads that `formatImpl` renders: `changes`, `default_settings`
-- (`SETTINGS x = DEFAULT`) and `query_parameters` (`SETTINGS param_x = '1'`). `updateTreeHashImpl`
-- hashed only the first two, so two `SETTINGS` clauses that format differently compared equal.
--
-- The query result cache is the observable consumer of that hash: it keys entries by the AST hash of
-- the (normalized) query. `param_*` is not a real setting and is not stripped by
-- `removeQueryResultCacheSettings`, so it stays part of the key - and before the fix two queries that
-- differ only in their parameter bindings collapsed into a single cache entry.
--
-- A unique `query_cache_tag` isolates the entries from any other test sharing the server-global query
-- cache; the cleanup `SYSTEM DROP QUERY CACHE TAG` keeps the test repeatable across re-runs.

SYSTEM DROP QUERY CACHE TAG '04666_set_params';

SELECT 1 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_tag = '04666_set_params', param_x = '1' FORMAT Null;
SELECT 1 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_tag = '04666_set_params', param_x = '2' FORMAT Null;

-- Two distinct bindings must occupy two distinct cache entries. Before the fix this printed 1.
SELECT count() FROM system.query_cache WHERE tag = '04666_set_params';

SYSTEM DROP QUERY CACHE TAG '04666_set_params';

-- The parameter name is hashed too, not just the value.
SELECT 1 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_tag = '04666_set_params', param_x = '1' FORMAT Null;
SELECT 1 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_tag = '04666_set_params', param_y = '1' FORMAT Null;

SELECT count() FROM system.query_cache WHERE tag = '04666_set_params';

SYSTEM DROP QUERY CACHE TAG '04666_set_params';

-- Sanity check: identical bindings still share one entry, i.e. the hash did not become unstable.
SELECT 1 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_tag = '04666_set_params', param_x = '1' FORMAT Null;
SELECT 1 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_tag = '04666_set_params', param_x = '1' FORMAT Null;

SELECT count() FROM system.query_cache WHERE tag = '04666_set_params';

SYSTEM DROP QUERY CACHE TAG '04666_set_params';
