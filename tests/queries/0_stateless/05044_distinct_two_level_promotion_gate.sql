-- Tags: long
-- The two-level conversion exists only to unlock the per-bucket parallel filter build, so it must not
-- happen when that build would not be used. Two ways it would not be: a chunk too small to keep more
-- than one worker busy (`distinct_two_level_parallel_build_min_rows`), and a single `LowCardinality`
-- key column, whose dictionary first-occurrence mask only the serial build consumes. Promoting anyway
-- pays the conversion rehash and buys nothing.
-- `distinct_two_level_threshold_bytes = 0` isolates the row-count path throughout.

SET max_threads = 8;

-- Small chunks: one worker, so no promotion at all.
SELECT DISTINCT number % 200000 AS k FROM numbers_mt(4000000)
    FORMAT Null SETTINGS max_block_size = 1000, distinct_two_level_threshold = 1000,
                         distinct_two_level_threshold_bytes = 0, log_comment = '05044_small_chunks';

-- Same query, default chunk size: promoted and built in parallel.
SELECT DISTINCT number % 200000 AS k FROM numbers_mt(4000000)
    FORMAT Null SETTINGS distinct_two_level_threshold = 1000,
                         distinct_two_level_threshold_bytes = 0, log_comment = '05044_big_chunks';

SYSTEM FLUSH LOGS;

-- Small chunks: never converted, so nothing was paid for an unused parallel build.
SELECT ProfileEvents['DistinctHashTablesInitializedAsTwoLevel'] = 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05044_small_chunks' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- Big chunks: converted, and the parallel build actually ran - so the gate above is not just
-- disabling the feature outright.
SELECT ProfileEvents['DistinctHashTablesInitializedAsTwoLevel'] > 0
   AND ProfileEvents['DistinctTwoLevelParallelFilterBuilds'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05044_big_chunks' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- Both chunk sizes must give the same distinct set.
SELECT
(
    SELECT count() FROM (SELECT DISTINCT number % 200000 AS k FROM numbers_mt(4000000)) SETTINGS max_block_size = 1000, distinct_two_level_threshold = 1000
) = (
    SELECT count() FROM (SELECT DISTINCT number % 200000 AS k FROM numbers_mt(4000000)) SETTINGS distinct_two_level_threshold = 1000
);

-- A single `LowCardinality` key column with a shared dictionary: the mask path, never promoted.
DROP TABLE IF EXISTS t_05044_lc;
CREATE TABLE t_05044_lc (lc LowCardinality(String)) ENGINE = MergeTree ORDER BY lc;
INSERT INTO t_05044_lc SELECT toLowCardinality(toString(number % 50000)) FROM numbers_mt(2000000);
OPTIMIZE TABLE t_05044_lc FINAL;

SELECT DISTINCT lc FROM t_05044_lc
    FORMAT Null SETTINGS distinct_two_level_threshold = 1000,
                         distinct_two_level_threshold_bytes = 0, log_comment = '05044_lowcardinality';

SYSTEM FLUSH LOGS;

SELECT ProfileEvents['DistinctTwoLevelParallelFilterBuilds'] = 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05044_lowcardinality' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT count() FROM (SELECT DISTINCT lc FROM t_05044_lc);

DROP TABLE t_05044_lc;
