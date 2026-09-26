-- Tests that the query condition cache key includes the changed settings in the entry hash (issue #117308)

SET parallel_replicas_local_plan = 1;
SET use_query_condition_cache = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (d DateTime, s String) ENGINE = MergeTree ORDER BY tuple() SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_string_columns = 0;
INSERT INTO tab SELECT toDateTime('2024-01-01 00:00:00', 'UTC'), 'haystack' FROM numbers(1_000_000); -- the QCC caches nothing for less data

SELECT '--- formatDateTime';

SELECT count() FROM tab WHERE formatDateTime(d, '%f') = '0' SETTINGS formatdatetime_f_prints_single_zero = 0;
SELECT count() FROM tab WHERE formatDateTime(d, '%f') = '0' SETTINGS formatdatetime_f_prints_single_zero = 1;

-- The other way round.
SELECT count() FROM tab WHERE formatDateTime(d, '%f') = '000000' SETTINGS formatdatetime_f_prints_single_zero = 1;
SELECT count() FROM tab WHERE formatDateTime(d, '%f') = '000000' SETTINGS formatdatetime_f_prints_single_zero = 0;

-- `log_comment` is ignored, checked in system.query_log
SELECT count() FROM tab WHERE formatDateTime(d, '%f') = '0000' SETTINGS formatdatetime_f_prints_single_zero = 0, log_comment = '05218_repeated_1';
SELECT count() FROM tab WHERE formatDateTime(d, '%f') = '0000' SETTINGS formatdatetime_f_prints_single_zero = 0, log_comment = '05218_repeated_2';

SELECT '--- locate';

SELECT count() FROM tab WHERE locate(s, 'stack') = 4 SETTINGS function_locate_has_mysql_compatible_argument_order = 0;
SELECT count() FROM tab WHERE locate(s, 'stack') = 4 SETTINGS function_locate_has_mysql_compatible_argument_order = 1;

SELECT count() FROM tab WHERE locate(s, 'stack') = 4 SETTINGS function_locate_has_mysql_compatible_argument_order = 1;
SELECT count() FROM tab WHERE locate(s, 'stack') = 4 SETTINGS function_locate_has_mysql_compatible_argument_order = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0,
    ProfileEvents['QueryConditionCacheMisses'] > 0
FROM system.query_log
WHERE type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('05218_repeated_1', '05218_repeated_2')
ORDER BY log_comment;

DROP TABLE tab;
