-- Tags: no-fasttest
-- Tag no-fasttest: requires S3 object storage (minio).

-- Test for use_indexes_refiner_in_read_pools with parts on a remote disk: mark ranges are only
-- shrunk from the edges or dropped entirely, never split on interior marks pruned by a skip
-- index. Splitting a range multiplies IO requests with high latency on remote storage, so
-- interior pruned marks stay in the range and are skipped granule by granule during reading.

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_indexes_refiner_in_read_pools = 1;
SET max_rows_to_read = 0;
SET enable_parallel_replicas = 0;
-- Pin the prefetched read pool for remote reads (these settings are randomized in CI).
SET allow_prefetched_read_pool_for_remote_filesystem = 1;
SET remote_filesystem_read_method = 'threadpool';
SET remote_filesystem_read_prefetch = 1;
SET use_page_cache_for_disks_without_file_cache = 0;
SET use_page_cache_for_object_storage = 0;
-- A single read task covering the whole part (256 marks), so the mark accounting below is exact.
SET max_threads = 1;
SET filesystem_prefetch_step_marks = 256;
SET filesystem_prefetch_step_bytes = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS t_skip_idx_refiner_s3;

CREATE TABLE t_skip_idx_refiner_s3
(
    id UInt64,
    region String,
    value UInt64,
    INDEX region_idx region TYPE set(8) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS storage_policy = 's3_no_cache',
    index_granularity = 16, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0;

-- 256 marks of 16 rows. The 'rare' region fully occupies marks 8, 24, 40, ..., 248: the skip
-- index prunes 240 of 256 marks, but 225 of them lie between the first and the last matching
-- mark, so only the 15 edge marks (0-7 and 249-255) may be dropped from the single read range.
INSERT INTO t_skip_idx_refiner_s3
SELECT number, if(intDiv(number, 16) % 16 = 8, 'rare', 'common'), number * 10
FROM numbers(4096);

-- Randomized insert block size settings may split the insert into several parts.
OPTIMIZE TABLE t_skip_idx_refiner_s3 FINAL;

SELECT /* skip_refiner_s3_rare */ count(), min(id), max(id), sum(value)
FROM t_skip_idx_refiner_s3 WHERE region = 'rare';

-- A fully pruned range is dropped entirely (shrinking from both edges meets in the middle).
SELECT /* skip_refiner_s3_nonexistent */ count()
FROM t_skip_idx_refiner_s3 WHERE region = 'nonexistent';

SYSTEM FLUSH LOGS query_log;

-- Only the 15 edge marks are dropped; splitting the range would drop all 240 pruned marks.
-- RemoteFSPrefetches > 0 proves that reading went through the prefetched read pool.
SELECT
    ProfileEvents['ReadPoolRangeRefinerDroppedMarks'] AS dropped_marks,
    ProfileEvents['RemoteFSPrefetches'] > 0 AS used_prefetched_pool
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%skip_refiner_s3_rare%'
    AND query NOT LIKE '%query_log%';

SELECT
    ProfileEvents['ReadPoolRangeRefinerDroppedMarks'] AS dropped_marks,
    ProfileEvents['ReadPoolRangeRefinerDroppedCuts'] AS dropped_cuts
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%skip_refiner_s3_nonexistent%'
    AND query NOT LIKE '%query_log%';

DROP TABLE t_skip_idx_refiner_s3;
