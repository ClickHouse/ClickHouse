-- The query condition cache keys a granule verdict by a hash of the filter expression, which named
-- the conversion function and its result type but not the settings the conversion captured when it
-- was built. Two sessions that differ only in `precise_float_parsing` therefore shared one key, and
-- the one running at defaults was served the other's "no matching rows" verdict: every row of a
-- matching granule silently disappeared from its result.

DROP TABLE IF EXISTS t_qcc_conversion_settings;
CREATE TABLE t_qcc_conversion_settings (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_qcc_conversion_settings SELECT number, '1.1e-300' FROM numbers(20000);

-- The two settings parse this string to values that differ by one ULP, so the predicate is true
-- under the default and false without it, while the comparison constant is the same either way.
SELECT 'every row matches at defaults', count() FROM t_qcc_conversion_settings
WHERE toFloat64(s) = 1.1e-300 SETTINGS use_query_condition_cache = 0;

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'and none without precise parsing', count() FROM t_qcc_conversion_settings
WHERE toFloat64(s) = 1.1e-300 SETTINGS use_query_condition_cache = 1, precise_float_parsing = 0;

SELECT 'the cached verdict is not served to the default session', count() FROM t_qcc_conversion_settings
WHERE toFloat64(s) = 1.1e-300 SETTINGS use_query_condition_cache = 1;

SELECT 'and it is still not, the other way round';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_conversion_settings
WHERE toFloat64(s) = 1.1e-300 SETTINGS use_query_condition_cache = 1;
SELECT count() FROM t_qcc_conversion_settings
WHERE toFloat64(s) = 1.1e-300 SETTINGS use_query_condition_cache = 1, precise_float_parsing = 0;

SELECT 'and a repeat of the default session keeps its own verdict', count()
FROM t_qcc_conversion_settings WHERE toFloat64(s) = 1.1e-300 SETTINGS use_query_condition_cache = 1;

-- The key separates the two settings without becoming per-session: a repeat of the same query reuses
-- the entry it wrote, and only the session with the other setting adds one of its own.
SELECT 'a filter that matches nothing under either setting';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_conversion_settings WHERE toFloat64(s) = 1.2e-300 SETTINGS use_query_condition_cache = 1;
SELECT 'entries', count() FROM system.query_condition_cache;
SELECT count() FROM t_qcc_conversion_settings WHERE toFloat64(s) = 1.2e-300 SETTINGS use_query_condition_cache = 1;
SELECT 'entries after the same query again', count() FROM system.query_condition_cache;
SELECT count() FROM t_qcc_conversion_settings WHERE toFloat64(s) = 1.2e-300
SETTINGS use_query_condition_cache = 1, precise_float_parsing = 0;
SELECT 'entries after the other setting', count() FROM system.query_condition_cache;

DROP TABLE t_qcc_conversion_settings;
