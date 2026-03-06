-- Tags: no-random-merge-tree-settings
-- - no-random-merge-tree-settings -- may change amount of granules

DROP TABLE IF EXISTS text_idx_data;

CREATE TABLE text_idx_data
(
    key UInt64,
    text String,
    INDEX idx(text) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY key
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

-- Insert 100 rows; only key=0 and key=50 contain 'needle'
INSERT INTO text_idx_data SELECT
    number,
    if(number % 50 = 0, 'needle in a haystack', 'just some regular text')
FROM numbers(100);

-- { echo }

-- Text index narrows ranges (only granules 0 and 25 contain 'needle')
SELECT part_name, ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), text_idx_data, hasToken(text, 'needle'));

-- Without predicate all granules are returned
SELECT part_name, ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), text_idx_data);

-- Verify query correctness
SELECT count() FROM text_idx_data WHERE hasToken(text, 'needle');

-- Token that does not exist: part present but empty ranges
SELECT part_name, ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), text_idx_data, hasToken(text, 'nonexistent'));

-- Combined PK + text index
SELECT part_name, ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), text_idx_data, key < 10 AND hasToken(text, 'needle'));

-- extra_data contains serialized text index analyzer state (keyed by index name)
SELECT part_name, mapKeys(extra_data), notEmpty(extra_data['idx']) FROM mergeTreeAnalyzeIndexes(currentDatabase(), text_idx_data, hasToken(text, 'needle'));

-- Without text index condition, extra_data is empty
SELECT part_name, extra_data FROM mergeTreeAnalyzeIndexes(currentDatabase(), text_idx_data);

-- { echoOff }
DROP TABLE text_idx_data;

-- Test distributed index analysis with text index (indirectly via a query)
DROP TABLE IF EXISTS text_idx_dist;

CREATE TABLE text_idx_dist
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

SYSTEM STOP MERGES text_idx_dist;
INSERT INTO text_idx_dist SELECT number, if(number % 10000 = 0, 'needle in a haystack', 'just some regular text') FROM numbers(100000) SETTINGS max_block_size=10000, min_insert_block_size_rows=10000, max_insert_threads=1;

SET cluster_for_parallel_replicas = 'parallel_replicas';
SET max_parallel_replicas = 10;
SET allow_experimental_parallel_reading_from_replicas = 0;
SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 0;
SET distributed_index_analysis_for_non_shared_merge_tree = 1;
-- Avoid error on unavailable replica
SET send_logs_level = 'error';

-- { echo }

-- Without distributed index analysis
SELECT count() FROM text_idx_dist WHERE hasToken(text, 'needle') SETTINGS distributed_index_analysis = 0;

-- With distributed index analysis, result should be the same
SELECT count() FROM text_idx_dist WHERE hasToken(text, 'needle') SETTINGS distributed_index_analysis = 1;

-- { echoOff }

SYSTEM FLUSH LOGS query_log;
SELECT format(
    'distributed_index_analysis={}, DistributedIndexAnalysisScheduledReplicas>0={}',
    Settings['distributed_index_analysis'],
    ProfileEvents['DistributedIndexAnalysisScheduledReplicas'] > 0
)
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND is_initial_query
    AND has(Settings, 'distributed_index_analysis')
    AND endsWith(log_comment, '-' || currentDatabase())
ORDER BY event_time_microseconds;

DROP TABLE text_idx_dist;
