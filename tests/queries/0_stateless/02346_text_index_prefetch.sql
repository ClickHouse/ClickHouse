-- Tags: no-fasttest, no-parallel-replicas

SET use_skip_indexes_on_data_read = 1;
SET allow_prefetched_read_pool_for_remote_filesystem = 1;
SET remote_filesystem_read_prefetch = 1;
SET remote_filesystem_read_method = 'threadpool';
SET max_rows_to_read = 0;
SET filesystem_prefetch_step_bytes = 0;
SET filesystem_prefetch_step_marks = 0;
SET enable_filesystem_cache = 1;
SET read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0;
-- When merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability is on splitPartsRanges
-- skips extracting non-intersecting ranges from level-0 parts (which all 3 parts are, since each has a single INSERT).
-- All parts end up in the "intersecting" bucket, read via MergeTreeReadPoolInOrder which never calls
-- prefetchBeginOfRange — so zero prefetched reads. Fixed by setting the injection probability to 0.
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;


DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx_str str TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 8
)
ENGINE = MergeTree ORDER BY id PARTITION BY id
SETTINGS storage_policy = 's3_cache';

INSERT INTO tab SELECT 1, arrayStringConcat(arrayMap(x -> toString(number + x * 2), range(5)), ' ') FROM numbers(0, 100000);
INSERT INTO tab SELECT 2, arrayStringConcat(arrayMap(x -> toString(number + x * 2), range(5)), ' ') FROM numbers(100000, 100000);
INSERT INTO tab SELECT 3, arrayStringConcat(arrayMap(x -> toString(number + x * 2), range(5)), ' ') FROM numbers(200000, 100000);

SELECT count(), sum(id) FROM tab WHERE hasAnyTokens(str, ['34567', '134567', '234567']);

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['RemoteFSPrefetchedReads'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase() AND query LIKE '%SELECT count(), sum(id) FROM tab%' AND type = 'QueryFinish';

DROP TABLE tab;
