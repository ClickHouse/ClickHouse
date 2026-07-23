DROP TABLE IF EXISTS text_index_flush_settings;

SELECT name, value
FROM system.merge_tree_settings
WHERE name IN ('text_index_max_memory_usage_before_flush', 'text_index_max_processed_tokens_before_flush')
ORDER BY name;

CREATE TABLE text_index_flush_settings
(
    id UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 1,
    text_index_max_memory_usage_before_flush = 1,
    text_index_max_processed_tokens_before_flush = 1;

INSERT INTO text_index_flush_settings SELECT number, concat('token', toString(number)) FROM numbers(100);

ALTER TABLE text_index_flush_settings
    ADD INDEX idx s TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 100000000;
ALTER TABLE text_index_flush_settings
    MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, max_block_size = 1;

SELECT count()
FROM text_index_flush_settings
WHERE hasToken(s, 'token42')
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE text_index_flush_settings;
