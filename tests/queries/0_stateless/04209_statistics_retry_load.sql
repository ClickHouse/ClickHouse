-- Tags: no-parallel, no-replicated-database, no-random-settings
-- Tag no-parallel: toggles a server-global failpoint.
-- Tag no-replicated-database: hypothetical indexes are session-scoped and not replicated.
-- Tag no-random-settings: keeps the statistics paths and settings deterministic.

SET allow_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;

SYSTEM DISABLE FAILPOINT merge_tree_load_statistics_throw;
DROP TABLE IF EXISTS t_full;
DROP TABLE IF EXISTS t_packed;

CREATE TABLE t_full (a UInt64 STATISTICS(basic), b UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1,
         refresh_statistics_interval = 0;

CREATE TABLE t_packed (a UInt64 STATISTICS(basic), b UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = '1G',
         max_bytes_to_merge_at_max_space_in_pool = 1,
         refresh_statistics_interval = 0;

INSERT INTO t_full SELECT number, number FROM numbers(1000);
INSERT INTO t_full SELECT number + 1000000, number FROM numbers(1000);
INSERT INTO t_packed SELECT number, number FROM numbers(1000);
INSERT INTO t_packed SELECT number + 1000000, number FROM numbers(1000);

DETACH TABLE t_full;
ATTACH TABLE t_full;
DETACH TABLE t_packed;
ATTACH TABLE t_packed;

SYSTEM ENABLE FAILPOINT merge_tree_load_statistics_throw;

-- Both statistics storage representations must propagate a deserialize failure.
SELECT count() FROM t_full WHERE a > 500000
SETTINGS use_statistics_for_part_pruning = 1,
         use_statistics_cache = 1,
         log_comment = '04209_statistics_retry_load_error_full'; -- { serverError CANNOT_READ_ALL_DATA }

SELECT count() FROM t_packed WHERE a > 500000
SETTINGS use_statistics_for_part_pruning = 1,
         use_statistics_cache = 0,
         log_comment = '04209_statistics_retry_load_error_packed'; -- { serverError CANNOT_READ_ALL_DATA }

CREATE HYPOTHETICAL INDEX idx_a ON t_full (a) TYPE minmax GRANULARITY 1;
EXPLAIN WHATIF empirical = 0 SELECT * FROM t_full WHERE a > 500000
SETTINGS use_statistics_for_part_pruning = 0,
         log_comment = '04209_statistics_retry_load_error_whatif'; -- { serverError CANNOT_READ_ALL_DATA }

SYSTEM DISABLE FAILPOINT merge_tree_load_statistics_throw;
SYSTEM FLUSH LOGS query_log;

-- Preserve the diagnostic contract for packed and separate statistics files.
SELECT
    countIf(
        log_comment = '04209_statistics_retry_load_error_full'
        AND position(exception, '(while loading statistics for column a from file statistics_a.stats in packed file statistics.packed of part all_1_1_0)') > 0) = 1,
    countIf(
        log_comment = '04209_statistics_retry_load_error_packed'
        AND position(exception, '(while loading statistics for column a from file statistics_a.stats in part all_1_1_0)') > 0) = 1,
    countIf(
        log_comment = '04209_statistics_retry_load_error_whatif'
        AND position(exception, '(while loading statistics for column a from file statistics_a.stats in packed file statistics.packed of part all_1_1_0)') > 0) = 1
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment LIKE '04209_statistics_retry_load_error_%';

-- After the failure, both representations must load valid statistics and prune all parts.
SELECT count() FROM t_full WHERE a > 2000000
SETTINGS use_statistics_for_part_pruning = 1,
         use_statistics_cache = 1,
         log_comment = '04209_statistics_retry_load_t_full';

SELECT count() FROM t_packed WHERE a > 2000000
SETTINGS use_statistics_for_part_pruning = 1,
         use_statistics_cache = 1,
         log_comment = '04209_statistics_retry_load_t_packed';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['SelectedParts']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment IN ('04209_statistics_retry_load_t_full', '04209_statistics_retry_load_t_packed')
  AND type = 'QueryFinish'
ORDER BY log_comment;

DROP TABLE t_full;
DROP TABLE t_packed;
