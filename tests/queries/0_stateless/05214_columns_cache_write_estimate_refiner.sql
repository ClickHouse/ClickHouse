-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas, no-replicated-database
-- - no-parallel -- `SYSTEM DROP COLUMNS CACHE` is server-wide
-- - no-random-settings, no-random-merge-tree-settings -- the test sets the refiner, the estimate
--   budget and the part format itself
-- - no-parallel-replicas -- another replica would do the reading
-- - no-replicated-database -- the cache is per server

-- The columns cache write estimate charges every mark the read selects, before anything is read.
-- A read ranges refiner (`use_indexes_refiner_in_read_pools`) drops marks of those ranges when it
-- cuts a task, and it can drop almost all of them - that is what it is for. Charging the dropped
-- marks would stop a selective read from caching the little it does read, so a pool that has a
-- refiner is not gated on the estimate at all; what it writes is bounded by
-- `columns_cache_max_bytes_to_write_to_cache` instead.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
-- The CI test profile sets `max_rows_to_read` (tests/config/users.d/limits.yaml), and with
-- `read_overflow_mode = 'throw'` that disables applying skip indexes at data-read time entirely,
-- which would leave the ranges refiner with nothing to drop.
SET max_rows_to_read = 0;
-- Pin the plain read pool, and one task, so the cold and the hot read agree on the ranges the
-- cache entries cover.
SET enable_parallel_replicas = 0;
SET allow_prefetched_read_pool_for_local_filesystem = 0, allow_prefetched_read_pool_for_remote_filesystem = 0;
SET max_threads = 1;

DROP TABLE IF EXISTS t_cc_estimate_refiner;

CREATE TABLE t_cc_estimate_refiner
(
    id UInt64,
    region String,
    payload String,
    INDEX region_idx region TYPE set(8) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

-- About 130 bytes per row uncompressed over 25 granules, of which `region = 'rare'` keeps one.
INSERT INTO t_cc_estimate_refiner
SELECT number, if(number BETWEEN 100000 AND 100010, 'rare', 'common'), repeat('a', 100)
FROM numbers(200000);

OPTIMIZE TABLE t_cc_estimate_refiner FINAL;

SELECT sum(data_uncompressed_bytes) > 20000000
FROM system.parts WHERE database = currentDatabase() AND table = 't_cc_estimate_refiner' AND active;

SYSTEM DROP COLUMNS CACHE;

-- A budget below the uncompressed size of the whole part but far above the surviving granule.
-- Without the refiner every selected mark is charged, the gate trips and nothing is written.
SELECT max(payload) != '' FROM t_cc_estimate_refiner WHERE region = 'rare'
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1,
    use_indexes_refiner_in_read_pools = 0,
    columns_cache_max_estimated_bytes_to_write_to_cache = 4000000;

SELECT 'without refiner', count() FROM system.columns_cache WHERE database = currentDatabase();

SYSTEM DROP COLUMNS CACHE;

-- With the refiner the marks it drops are not charged, so the surviving range is cached.
SELECT max(payload) != '' FROM t_cc_estimate_refiner WHERE region = 'rare'
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1,
    use_indexes_refiner_in_read_pools = 1,
    columns_cache_max_estimated_bytes_to_write_to_cache = 4000000, log_comment = '05214_cold';

SELECT 'with refiner', count() > 0 FROM system.columns_cache WHERE database = currentDatabase();

-- And the repeated read is served from it.
SELECT max(payload) != '' FROM t_cc_estimate_refiner WHERE region = 'rare'
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1,
    use_indexes_refiner_in_read_pools = 1,
    columns_cache_max_estimated_bytes_to_write_to_cache = 4000000, log_comment = '05214_hot';

SYSTEM FLUSH LOGS query_log;

-- `dropped_marks` keeps the test honest: it fails if the refiner never engaged, in which case the
-- estimate would not have been an upper bound in the first place. `served_from_cache` must be 0
-- for the cold read and 1 for the hot one, so the test cannot pass on a build where the cache
-- never engages either.
SELECT
    log_comment,
    ProfileEvents['ReadPoolRangeRefinerDroppedMarks'] > 0 AS dropped_marks,
    ProfileEvents['ColumnsCacheHits'] > 0 AS served_from_cache
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment IN ('05214_cold', '05214_hot')
  AND type = 'QueryFinish'
  AND event_time >= now() - INTERVAL 5 MINUTE
ORDER BY log_comment;

DROP TABLE t_cc_estimate_refiner;
