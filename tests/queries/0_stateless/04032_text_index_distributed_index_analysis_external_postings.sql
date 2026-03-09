-- Tags: no-random-merge-tree-settings
-- - no-random-merge-tree-settings -- may change amount of granules

-- Test distributed index analysis with text index when tokens have
-- non-embedded (external) postings. This exercises the serialization
-- path in serializeStateBinary / deserializeFromStateBinary where
-- external postings must be written after the has_postings flag.
-- A token with cardinality in range (6, 12] gets RawPostings + SingleBlock
-- (not embedded), which triggers addPostings in analyzePostings, populating
-- token_postings in the analyzer state.

DROP TABLE IF EXISTS text_idx_ext_postings;

CREATE TABLE text_idx_ext_postings
(
    key UInt64,
    text String,
    INDEX idx(text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY key
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = '1G',
         distributed_index_analysis_min_parts_to_activate = 0,
         distributed_index_analysis_min_indexes_bytes_to_activate = 0;

SYSTEM STOP MERGES text_idx_ext_postings;

-- Insert 10 parts x 10000 rows. 'needle' appears every 1000 rows → 10 matches per part.
-- Cardinality 10 is in (6, 12] range → non-embedded, raw, single-block external postings.
INSERT INTO text_idx_ext_postings SELECT
    number,
    if(number % 1000 = 0, 'needle in a haystack', 'just some regular text')
FROM numbers(100000)
SETTINGS max_block_size = 10000, min_insert_block_size_rows = 10000, max_insert_threads = 1;

SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET max_parallel_replicas = 3;
SET allow_experimental_parallel_reading_from_replicas = 0;
SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 0;
SET distributed_index_analysis_for_non_shared_merge_tree = 1;

-- { echo }

-- Without distributed index analysis
SELECT count() FROM text_idx_ext_postings WHERE hasToken(text, 'needle') SETTINGS distributed_index_analysis = 0;

-- With distributed index analysis (exercises external postings serialization)
SELECT count() FROM text_idx_ext_postings WHERE hasToken(text, 'needle') SETTINGS distributed_index_analysis = 1;

-- { echoOff }

DROP TABLE text_idx_ext_postings;
