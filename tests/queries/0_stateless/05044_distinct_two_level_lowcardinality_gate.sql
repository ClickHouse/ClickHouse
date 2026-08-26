-- Tags: long
-- A single `LowCardinality` key column is deduplicated through the dictionary first-occurrence mask,
-- which only the serial filter build consumes. Promoting such a set to two-level would therefore pay
-- the conversion rehash and never reach the per-bucket parallel build, so the promotion must be
-- skipped entirely. Control: the same query over a plain `String` key does get promoted.
-- `distinct_two_level_threshold_bytes = 0` isolates the row-count path.

SET max_threads = 8;

SELECT DISTINCT toLowCardinality(toString(number % 100000)) FROM numbers_mt(5000000)
    FORMAT Null SETTINGS distinct_two_level_threshold = 1000, distinct_two_level_threshold_bytes = 0,
                         log_comment = '05044_distinct_lc_gate';

SELECT DISTINCT toString(number % 100000) FROM numbers_mt(5000000)
    FORMAT Null SETTINGS distinct_two_level_threshold = 1000, distinct_two_level_threshold_bytes = 0,
                         log_comment = '05044_distinct_plain_control';

SYSTEM FLUSH LOGS;

-- LowCardinality key: never promoted.
SELECT ProfileEvents['DistinctHashTablesInitializedAsTwoLevel'] = 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05044_distinct_lc_gate' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- Plain String key: promoted, so the gate above is not just disabling the feature outright.
SELECT ProfileEvents['DistinctHashTablesInitializedAsTwoLevel'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05044_distinct_plain_control' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- The LowCardinality result must still be correct without the two-level path.
SELECT count() FROM (SELECT DISTINCT toLowCardinality(toString(number % 100000)) FROM numbers_mt(5000000));
