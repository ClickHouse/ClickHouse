-- Tags: no-random-merge-tree-settings
-- `no-random-merge-tree-settings`: this test exercises the temporary text-index segment builder.

DROP TABLE IF EXISTS text_index_flush_settings;
SET enable_parallel_replicas = 0;

SELECT name, value
FROM system.merge_tree_settings
WHERE name IN ('text_index_max_memory_usage_before_flush', 'text_index_max_processed_tokens_before_flush')
ORDER BY name;

SELECT tupleElement(change, 'previous_value'), tupleElement(change, 'new_value')
FROM system.settings_changes
ARRAY JOIN changes AS change
WHERE type = 'MergeTree'
    AND version = '26.8'
    AND tupleElement(change, 'name') = 'text_index_max_memory_usage_before_flush';

CREATE TABLE text_index_flush_settings
(
    id UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 1,
    min_bytes_for_wide_part = 0,
    text_index_max_memory_usage_before_flush = 1073741824,
    text_index_max_processed_tokens_before_flush = 1;

INSERT INTO text_index_flush_settings SELECT number, concat('token', toString(number)) FROM numbers(4);

ALTER TABLE text_index_flush_settings
    ADD INDEX idx s TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 100000000;
ALTER TABLE text_index_flush_settings
    MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, max_block_size = 2;

SYSTEM FLUSH LOGS part_log;

SELECT 'token_limit_flushed', ProfileEvents['TextIndexTemporarySegmentsWritten'] > 1
FROM system.part_log
WHERE event_date >= yesterday()
    AND event_time >= now() - 600
    AND database = currentDatabase()
    AND table = 'text_index_flush_settings'
    AND event_type = 'MutatePart'
    AND error = 0
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT count()
FROM text_index_flush_settings
WHERE hasToken(s, 'token2')
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE text_index_flush_settings;
