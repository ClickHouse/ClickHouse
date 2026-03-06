-- Tags: no-random-merge-tree-settings, no-random-settings, long
-- no-random-merge-tree-settings -- may change amount of granules
-- no-random-settings -- parallel replicas settings must be stable
-- long -- inserts 10M rows

-- This test verifies that preloaded text index granules computed during
-- index analysis on the initiator are correctly propagated through the
-- parallel replicas protocol to worker replicas.
--
-- Without granule propagation, workers would have null granules and throw
-- a LOGICAL_ERROR in MergeTreeReaderTextIndex.

DROP TABLE IF EXISTS text_idx_pr;

CREATE TABLE text_idx_pr
(
    key UInt64,
    text String,
    INDEX idx(text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY key
SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = '1G';

SYSTEM STOP MERGES text_idx_pr;

INSERT INTO text_idx_pr SELECT number, if(number % 2 = 0, 'needle', 'haystack') FROM numbers(10000000) SETTINGS max_insert_threads = 1;

-- Baseline: correct result without parallel replicas
SELECT count()
FROM text_idx_pr
WHERE hasToken(text, 'needle')
SETTINGS
    enable_parallel_replicas = 0,
    query_plan_direct_read_from_text_index = 1;

-- With parallel replicas + direct read from text index:
-- The initiator computes index granules locally, then propagates them through
-- the parallel replicas protocol (via serialized_index_granules in
-- RangesInDataPartDescription). Workers deserialize and use the granules.
SELECT count()
FROM text_idx_pr
WHERE hasToken(text, 'needle')
SETTINGS
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    allow_experimental_analyzer = 1,
    use_query_condition_cache = 0,
    enable_parallel_replicas = 2,
    max_parallel_replicas = 3,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_mark_segment_size = 1,
    parallel_replicas_local_plan = 0,
    query_plan_direct_read_from_text_index = 1,
    use_skip_indexes_on_data_read = 0;

DROP TABLE text_idx_pr;
