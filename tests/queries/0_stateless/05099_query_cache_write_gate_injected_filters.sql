-- Tests that the ordinary query cache safety gate (`checkCanWriteQueryResultCache`) inspects the
-- filters a query inherits from settings (`additional_table_filters`, `additional_result_filter`) and
-- not only the query text. Such a filter is part of the query the server runs and its text is the
-- same on every run, so a non-deterministic function or a system table inside it makes the result
-- just as unfit for caching as one written in the query itself. This is the plain query cache -
-- `query_cache_use_only_when_data_was_not_changed` is off here, so the consistency hash is not what
-- keeps these results out of the cache.

-- Each case uses its own `query_cache_tag`, so the `system.query_cache` counts below see only this
-- test's entries. No entry is ever stored under a tag whose count is expected to be zero, so the test
-- neither clears the server-wide cache (which would make it `no-parallel`) nor depends on what an
-- earlier run of it left behind.

DROP TABLE IF EXISTS t_05099;
CREATE TABLE t_05099 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_05099 VALUES (1), (2), (3);

-- A deterministic injected filter keeps the cache usable: the result is stored.
SELECT 'deterministic filter is cached';
SELECT x FROM t_05099 ORDER BY x SETTINGS additional_result_filter = 'x <= 2', use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_tag = '05099_deterministic';
SELECT count() > 0 FROM system.query_cache WHERE tag = '05099_deterministic';

-- A non-deterministic function inside an injected filter is reported the same way as one in the
-- query text: under the default `query_cache_nondeterministic_function_handling = 'throw'` the
-- query fails instead of silently caching a result that changes on every run.
SELECT 'non-deterministic injected filter';
SELECT count() FROM t_05099 SETTINGS additional_table_filters = {'t_05099': 'x <= 100 + rand() * 0'}, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_tag = '05099_throw'; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT x FROM t_05099 ORDER BY x SETTINGS additional_result_filter = 'x <= 100 + rand() * 0', use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_tag = '05099_throw'; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count() FROM system.query_cache WHERE tag = '05099_throw';

-- `'ignore'` runs the query without caching its result, `'save'` caches it - the two escape hatches
-- documented for the setting work for an injected filter as well as for the query text.
SELECT count() FROM t_05099 SETTINGS additional_table_filters = {'t_05099': 'x <= 100 + rand() * 0'}, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_tag = '05099_nondeterministic_ignore', query_cache_nondeterministic_function_handling = 'ignore';
SELECT count() FROM system.query_cache WHERE tag = '05099_nondeterministic_ignore';
SELECT count() FROM t_05099 SETTINGS additional_table_filters = {'t_05099': 'x <= 100 + rand() * 0'}, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_tag = '05099_nondeterministic_save', query_cache_nondeterministic_function_handling = 'save';
SELECT count() > 0 FROM system.query_cache WHERE tag = '05099_nondeterministic_save';

-- A system table read by an injected filter's subquery is treated like one in the query text: its
-- contents change on their own, so the default `query_cache_system_table_handling = 'throw'` rejects
-- the query rather than caching a snapshot of it.
SELECT 'system table in an injected filter';
SELECT count() FROM t_05099 SETTINGS additional_table_filters = {'t_05099': 'x IN (SELECT 1 FROM system.one)'}, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_tag = '05099_system_throw'; -- { serverError QUERY_CACHE_USED_WITH_SYSTEM_TABLE }
SELECT count() FROM system.query_cache WHERE tag = '05099_system_throw';
SELECT count() FROM t_05099 SETTINGS additional_table_filters = {'t_05099': 'x IN (SELECT 1 FROM system.one)'}, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_tag = '05099_system_ignore', query_cache_system_table_handling = 'ignore';
SELECT count() FROM system.query_cache WHERE tag = '05099_system_ignore';

-- A filter that cannot be parsed cannot be checked, so the gate fails closed. The query itself fails
-- on the same filter later, which is what the client sees.
SELECT 'unparsable injected filter';
SELECT count() FROM t_05099 SETTINGS additional_table_filters = {'t_05099': 'x <='}, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_tag = '05099_unparsable'; -- { serverError SYNTAX_ERROR }
SELECT count() FROM system.query_cache WHERE tag = '05099_unparsable';

DROP TABLE t_05099;
