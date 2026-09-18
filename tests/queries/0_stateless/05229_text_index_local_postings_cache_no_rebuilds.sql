-- Tags: no-parallel-replicas
-- no-parallel-replicas: parallel replicas split the part between replicas, each with its own query-local
--   cache, so the segments at the replica boundaries are built more than once and the count below is not
--   deterministic.

-- The postings cache that a query creates for itself when `use_text_index_postings_cache = 0` is sized as 10%
-- of `max_memory_usage`. With more than one thread a part is read in tasks and the text index reader drops its
-- lazy cursors at every task, so a posting list segment spanning several tasks is prepared once per task and
-- has to be served from that cache after the first build. The cache used to be SLRU with the protected queue
-- as large as the whole cache: a segment prepared twice was pinned there for good, and once the pinned entries
-- filled the cache every later insert was evicted at once, so every task rebuilt its segments from disk. With
-- plain LRU the readers sweep the segments in task order and each segment is built exactly once even when the
-- segments of the query do not fit into the cache.

SET enable_full_text_index = 1;
SET text_index_posting_list_apply_mode = 'lazy';
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_optimize_count_from_text_index = 0;
SET use_query_condition_cache = 0;
SET use_text_index_postings_cache = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
-- One granule of 32 rows per read task, on remote disks too.
SET merge_tree_min_rows_for_concurrent_read = 32;
SET merge_tree_min_bytes_for_concurrent_read = 1;
SET merge_tree_min_read_task_size = 1;
SET merge_tree_min_bytes_per_task_for_remote_reading = 1;
SET log_queries = 1;
-- A single part: the segment count below assumes that every token has exactly 16384 postings in one part.
SET max_insert_threads = 1;

DROP TABLE IF EXISTS tab_local_postings_cache;

-- Every row has all 32 tokens, so each token has 16384 / 128 = 128 segments of 128 rows and every segment
-- spans 4 read tasks. The 4096 segments weigh about 4.8 MB in the cache, while `max_memory_usage = 30 MB`
-- gives the query-local cache 3 MB: more than one thread's half of the part, so no segment still in use can
-- be evicted, but less than the whole, so a policy that pins entries runs out of space.
CREATE TABLE tab_local_postings_cache
(
    k UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', posting_list_block_size = 128)
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 32, index_granularity_bytes = '10M', min_bytes_for_wide_part = 0;

INSERT INTO tab_local_postings_cache
SELECT number, arrayStringConcat(arrayMap(i -> 'tok' || toString(i), range(32)), ' ')
FROM numbers(16384);

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_local_postings_cache' AND active;

SELECT count() FROM tab_local_postings_cache
WHERE hasAllTokens(s, ['tok0', 'tok1', 'tok2', 'tok3', 'tok4', 'tok5', 'tok6', 'tok7', 'tok8', 'tok9', 'tok10', 'tok11', 'tok12', 'tok13', 'tok14', 'tok15',
                       'tok16', 'tok17', 'tok18', 'tok19', 'tok20', 'tok21', 'tok22', 'tok23', 'tok24', 'tok25', 'tok26', 'tok27', 'tok28', 'tok29', 'tok30', 'tok31'])
SETTINGS max_threads = 2, max_memory_usage = 30000000, log_comment = '05229_local_postings_cache';

SYSTEM FLUSH LOGS query_log;

-- Every segment was prepared by several tasks but read and decoded exactly once: 32 tokens * 128 segments.
SELECT
    ProfileEvents['TextIndexLazySegmentsBuilt'] AS segments_built,
    ProfileEvents['TextIndexLazySegmentsPrepared'] > ProfileEvents['TextIndexLazySegmentsBuilt'] AS segments_reused
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '05229_local_postings_cache';

DROP TABLE tab_local_postings_cache;
