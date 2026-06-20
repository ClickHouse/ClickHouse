-- Tags: no-parallel
-- Tag no-parallel: messes with the global query condition cache

-- Regression test for issue #104203:
-- The `QueryConditionCache` must not be written to from contexts that have a non-default
-- setting matching one of these explicit opt-in name patterns: `allow_suspicious_*`,
-- any name containing `relaxed`, or `allow_experimental_*` for settings outside the
-- `Production` tier (i.e. still in `Beta` or `Experimental`). Such settings can change
-- `PREWHERE` / `WHERE` evaluation outcomes (type coercion, constant folding, mark
-- filtering) without changing the action-graph hash that keys the cache. A "no marks
-- match" verdict produced under such a profile must not be served back to a query
-- running with default settings — that would silently return the wrong rows. Reads
-- from such contexts are still permitted: an entry written by a normal-context query
-- remains valid for any reader.

SET allow_experimental_analyzer = 1;
-- Ensure the `PREWHERE` write site is exercised (the same setup other QCC tests use).
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;

DROP TABLE IF EXISTS qcc_writable;

-- `add_minmax_index_for_numeric_columns=0`: otherwise the auto MinMax index would
-- prune marks before the QCC gets a chance to record a verdict.
CREATE TABLE qcc_writable (a UInt32, b UInt32)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1024, add_minmax_index_for_numeric_columns = 0;

-- 1 mio rows: the QCC only caches verdicts on tables of meaningful size.
INSERT INTO qcc_writable SELECT number, number FROM numbers(1_000_000);

SELECT '--- baseline: writes happen for normal contexts';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM qcc_writable WHERE b = 10000
SETTINGS use_query_condition_cache = 1
FORMAT Null;
SELECT count() > 0 FROM system.query_condition_cache;

SELECT '--- writes are disabled when allow_suspicious_low_cardinality_types is set';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM qcc_writable WHERE b = 10000
SETTINGS use_query_condition_cache = 1, allow_suspicious_low_cardinality_types = 1
FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SELECT '--- writes are disabled when an experimental setting is enabled';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM qcc_writable WHERE b = 10000
SETTINGS use_query_condition_cache = 1, allow_experimental_funnel_functions = 1
FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SELECT '--- writes are disabled when a Beta-tier allow_experimental_* setting is enabled (canonical name)';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM qcc_writable WHERE b = 10000
SETTINGS use_query_condition_cache = 1, allow_experimental_database_iceberg = 1
FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SELECT '--- writes are disabled when a Beta-tier allow_experimental_* alias is set (allow_experimental_lightweight_update -> enable_lightweight_update)';
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM qcc_writable WHERE b = 10000
SETTINGS use_query_condition_cache = 1, allow_experimental_lightweight_update = 0
FORMAT Null;
SELECT count() FROM system.query_condition_cache;

SELECT '--- reads are still served when the current context is not writable';
-- First populate the cache from a normal context.
SYSTEM CLEAR QUERY CONDITION CACHE;
SELECT count() FROM qcc_writable WHERE b = 10000
SETTINGS use_query_condition_cache = 1
FORMAT Null;
-- The relaxed-context query must still be able to READ the entry. The entry count
-- should remain non-zero (only writes are refused, reads are allowed).
SELECT count() FROM qcc_writable WHERE b = 10000
SETTINGS use_query_condition_cache = 1, allow_suspicious_low_cardinality_types = 1
FORMAT Null;
SELECT count() > 0 FROM system.query_condition_cache;

DROP TABLE qcc_writable;
