-- Tests that query_cache_use_only_when_data_was_not_changed (issue #108713) tracks the filters
-- injected from settings (`additional_table_filters`, `additional_result_filter`), which are applied
-- by the planner only after the query AST is walked: a subquery inside such a filter must invalidate
-- the cache entry when its table changes, even though it never appears in the query text, and a
-- non-deterministic injected filter must fail closed the same way as one in the query text.

-- The cache key includes the current database, so this test (running in its own database) does not
-- need to clear the server-wide query cache (which would require a no-parallel tag). The
-- system.query_cache checks use table names unique to this test.

DROP TABLE IF EXISTS t_04813_main;
DROP TABLE IF EXISTS t_04813_filter;
CREATE TABLE t_04813_main (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_04813_filter (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_04813_main VALUES (1), (2), (3);
INSERT INTO t_04813_filter VALUES (1);

-- A subquery inside `additional_table_filters` reads a table the query text never mentions. The
-- entry is stored, reused while nothing changed, and must be invalidated by an INSERT into the
-- filter's table even though the main table is unchanged (the reuse used to survive and return the
-- result filtered by the old contents).
SELECT 'additional_table_filters';
SELECT count() FROM t_04813_main SETTINGS additional_table_filters = {'t_04813_main': 'x IN (SELECT x FROM t_04813_filter)'}, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
SELECT count() > 0 FROM system.query_cache WHERE query LIKE '%t\_04813\_main%';
SELECT count() FROM t_04813_main SETTINGS additional_table_filters = {'t_04813_main': 'x IN (SELECT x FROM t_04813_filter)'}, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
INSERT INTO t_04813_filter VALUES (2);
SELECT count() FROM t_04813_main SETTINGS additional_table_filters = {'t_04813_main': 'x IN (SELECT x FROM t_04813_filter)'}, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;

-- `additional_result_filter` filters the rows of the query result. A deterministic filter without
-- table dependencies keeps the feature usable: the entry is stored, reused, and still invalidated by
-- a change of the query's own table. (A subquery inside `additional_result_filter` currently fails
-- in the planner with `Not-ready Set`, independently of the query cache, so it cannot be tested here;
-- the dependency walk covers it all the same.)
SELECT 'additional_result_filter';
SELECT x FROM t_04813_main ORDER BY x SETTINGS additional_result_filter = 'x <= 2', use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
SELECT count() > 0 FROM system.query_cache WHERE query LIKE '%additional\_result\_filter%' AND query LIKE '%t\_04813\_main%';
SELECT x FROM t_04813_main ORDER BY x SETTINGS additional_result_filter = 'x <= 2', use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;
INSERT INTO t_04813_main VALUES (0);
SELECT x FROM t_04813_main ORDER BY x SETTINGS additional_result_filter = 'x <= 2', use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;

-- A non-deterministic function inside an injected filter is subject to the same fail-closed rule as
-- one in the query text: the consistency hash cannot be computed, so the entry is not stored at all.
-- `query_cache_nondeterministic_function_handling = 'save'` disarms the ordinary query cache gate,
-- which sees these filters too and would otherwise reject the query before the consistency check runs
-- (that gate is covered by `05099_query_cache_write_gate_injected_filters`), so what is left here is
-- the consistency hash alone.
SELECT 'non-deterministic filters fail closed';
SELECT count() FROM t_04813_main SETTINGS additional_table_filters = {'t_04813_main': 'x <= 100 + rand() * 0'}, use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1, query_cache_nondeterministic_function_handling = 'save';
SELECT x FROM t_04813_main ORDER BY x SETTINGS additional_result_filter = 'x <= 2 + rand() * 0', use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1, query_cache_nondeterministic_function_handling = 'save';
SELECT count() FROM system.query_cache WHERE query LIKE '%rand()%' AND query LIKE '%t\_04813\_main%';

DROP TABLE t_04813_main;
DROP TABLE t_04813_filter;
