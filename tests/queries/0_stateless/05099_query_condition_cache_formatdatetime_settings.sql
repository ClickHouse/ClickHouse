-- Tags: no-parallel
-- Tag no-parallel: messes with the query condition cache

-- The `formatdatetime_*` settings change how `formatDateTime` evaluates without leaving a trace in the
-- condition's `ActionsDAG`, so two queries that differ only in them must not share a query condition
-- cache entry: the first one's "no marks match" verdict is wrong for the second one.
-- The same holds for `function_locate_has_mysql_compatible_argument_order`, which swaps the arguments of `locate`.

-- The query condition cache is only used with the analyzer.
SET enable_analyzer = 1;
SET use_query_condition_cache = 1;
-- Without a local plan the filter steps run as part of the remote queries, and this server's cache sees nothing.
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS t_qcc_formatdatetime;

-- The auto minmax indexes would answer before the cache, and the cache stores nothing for small parts.
CREATE TABLE t_qcc_formatdatetime (k UInt64, d DateTime, s String) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
INSERT INTO t_qcc_formatdatetime SELECT number, toDateTime('2024-05-05 10:00:00') + number % 86400, 'abc' FROM numbers(1000000);

SYSTEM DROP QUERY CONDITION CACHE;

-- `formatDateTime(d, '%f')` renders '000000' by default and '0' with the setting enabled, so the
-- condition matches no row under the first value and every row under the second one.
SELECT count() FROM t_qcc_formatdatetime WHERE formatDateTime(d, '%f') = '0' SETTINGS formatdatetime_f_prints_single_zero = 0;
SELECT count() FROM t_qcc_formatdatetime WHERE formatDateTime(d, '%f') = '0' SETTINGS formatdatetime_f_prints_single_zero = 1;
SELECT count() FROM t_qcc_formatdatetime WHERE formatDateTime(d, '%f') = '0' SETTINGS use_query_condition_cache = 0, formatdatetime_f_prints_single_zero = 1;

SYSTEM DROP QUERY CONDITION CACHE;

-- `%M` is the month name by default and the minute with the setting disabled.
SELECT count() FROM t_qcc_formatdatetime WHERE formatDateTime(d, '%M') = '00' SETTINGS formatdatetime_parsedatetime_m_is_month_name = 1;
SELECT count() FROM t_qcc_formatdatetime WHERE formatDateTime(d, '%M') = '00' SETTINGS formatdatetime_parsedatetime_m_is_month_name = 0;
SELECT count() FROM t_qcc_formatdatetime WHERE formatDateTime(d, '%M') = '00' SETTINGS use_query_condition_cache = 0, formatdatetime_parsedatetime_m_is_month_name = 0;

-- `locate(s, 'b')` is `position(s IN 'b')` with the MySQL argument order (needle first) and `position('b' IN s)`
-- without it, so it is 0 for every row under the first value and 2 under the second one.
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_formatdatetime WHERE locate(s, 'b') = 2 SETTINGS function_locate_has_mysql_compatible_argument_order = 1;
SELECT count() FROM t_qcc_formatdatetime WHERE locate(s, 'b') = 2 SETTINGS function_locate_has_mysql_compatible_argument_order = 0;
SELECT count() FROM t_qcc_formatdatetime WHERE locate(s, 'b') = 2 SETTINGS use_query_condition_cache = 0, function_locate_has_mysql_compatible_argument_order = 0;

-- A repeated query with the same settings still reads the cached verdict.
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_formatdatetime WHERE formatDateTime(d, '%f') = '0' FORMAT Null;
SELECT count() FROM t_qcc_formatdatetime WHERE formatDateTime(d, '%f') = '0' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryConditionCacheHits'], read_rows
FROM system.query_log
WHERE current_database = currentDatabase() AND query LIKE '%formatDateTime(d, ''%f'') = ''0'' FORMAT Null%' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_qcc_formatdatetime;
