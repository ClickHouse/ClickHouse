-- Tags: no-parallel
-- no-parallel: the query condition cache is server-wide and this test drops it.

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

-- A setting that is not a format setting and does not reach the conversion leaves the key alone.
SELECT count() FROM t_qcc_conversion_settings WHERE toFloat64(s) = 1.2e-300
SETTINGS use_query_condition_cache = 1, precise_float_parsing = 0, max_threads = 3;
SELECT 'entries after an unrelated setting', count() FROM system.query_condition_cache;

DROP TABLE t_qcc_conversion_settings;

-- The conversion captures the whole `FormatSettings`, and the key has to see every member of it: this
-- one is read only by the text parsing of an array. At the defaults a `NULL` element parses as the
-- default value, so the filter is false on every row and the "skip every granule" verdict is cached;
-- the session that parses strictly must still get its exception instead of that verdict.
DROP TABLE IF EXISTS t_qcc_captured_settings;
CREATE TABLE t_qcc_captured_settings (id UInt64, a String, s String, d DateTime) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_qcc_captured_settings SELECT number, '[1,NULL]', 'foo bar   ', toDateTime('2024-01-02 03:04:05') FROM numbers(20000);

SELECT 'a format setting the conversion reads while parsing';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_captured_settings WHERE CAST(a AS Array(UInt8)) = [1, 1] SETTINGS use_query_condition_cache = 1;
SELECT count() FROM t_qcc_captured_settings WHERE CAST(a AS Array(UInt8)) = [1, 1]
SETTINGS use_query_condition_cache = 1, input_format_null_as_default = 0; -- { serverError CANNOT_READ_ARRAY_FROM_TEXT }

-- Other functions that capture a setting contribute it the same way. `countMatches` counts two
-- matches of a pattern that can match the empty string unless told to stop at the first empty match.
SELECT 'countMatches';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_captured_settings WHERE countMatches(s, '[a-zA-Z]*') = 1 SETTINGS use_query_condition_cache = 1;
SELECT count() FROM t_qcc_captured_settings WHERE countMatches(s, '[a-zA-Z]*') = 1
SETTINGS use_query_condition_cache = 1, count_matches_stop_at_empty_match = 1;

-- A comparison reads a string literal as the other side's type by the settings it captured: the same
-- literal is the first of February in one session and the second of January in the other.
SELECT 'a comparison against a string literal';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_captured_settings WHERE d = '02/01/2024 03:04:05'
SETTINGS use_query_condition_cache = 1, cast_string_to_date_time_mode = 'best_effort_us';
SELECT count() FROM t_qcc_captured_settings WHERE d = '02/01/2024 03:04:05'
SETTINGS use_query_condition_cache = 1, cast_string_to_date_time_mode = 'best_effort';

DROP TABLE t_qcc_captured_settings;
