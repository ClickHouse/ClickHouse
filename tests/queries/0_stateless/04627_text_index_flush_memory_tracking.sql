-- Tags: no-random-merge-tree-settings
-- `no-random-merge-tree-settings`: this test exercises the temporary text-index segment builder.

DROP TABLE IF EXISTS text_index_flush_memory;
DROP TABLE IF EXISTS text_index_flush_memory_not_in;
SET enable_parallel_replicas = 0;

CREATE TABLE text_index_flush_memory
(
    s String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    allow_experimental_text_index_phrase_search = 1,
    merge_max_block_size = 4096,
    min_bytes_for_wide_part = 0,
    text_index_max_memory_usage_before_flush = 65536,
    text_index_max_processed_tokens_before_flush = 1000000000;

-- Every row holds a distinct token so that the builder grows through the arena and the token map.
-- Those allocations go through `Allocator`, which reports them to the memory tracker in every build.
-- A workload small enough to fit into the structures preallocated by the builder would grow only
-- through `operator new`, which the sanitizer runtimes replace with their own untracked version.
INSERT INTO text_index_flush_memory SELECT concat('token', toString(number)) FROM numbers(20000);

ALTER TABLE text_index_flush_memory
    ADD INDEX idx s TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1);
-- `max_block_size` is pinned so that the mutation always reads several blocks and the threshold
-- is evaluated more than once.
ALTER TABLE text_index_flush_memory
    MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, max_block_size = 4096;

SYSTEM FLUSH LOGS part_log;

SELECT 'memory_limit_flushed', ProfileEvents['TextIndexTemporarySegmentsWritten'] > 1
FROM system.part_log
WHERE event_date >= yesterday()
    AND event_time >= now() - 600
    AND database = currentDatabase()
    AND table = 'text_index_flush_memory'
    AND event_type = 'MutatePart'
    AND error = 0
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT count()
FROM text_index_flush_memory
WHERE hasToken(s, 'token1234')
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE text_index_flush_memory;

CREATE TABLE text_index_flush_memory_not_in
(
    s String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    allow_experimental_text_index_phrase_search = 1,
    merge_max_block_size = 4096,
    min_bytes_for_wide_part = 0,
    text_index_max_memory_usage_before_flush = 1024,
    text_index_max_processed_tokens_before_flush = 1000000000;

-- None of the keep-set tokens occur in the input. Their pre-seeded map must still count toward
-- the memory-based flush threshold before any token is accepted by the postprocessor.
INSERT INTO text_index_flush_memory_not_in
SELECT concat('input', toString(number)) FROM numbers(20000);

ALTER TABLE text_index_flush_memory_not_in
    ADD INDEX idx s TYPE text(
        tokenizer = splitByNonAlpha,
        postprocessor = if(s NOT IN (
            'keep_00', 'keep_01', 'keep_02', 'keep_03', 'keep_04', 'keep_05', 'keep_06', 'keep_07',
            'keep_08', 'keep_09', 'keep_10', 'keep_11', 'keep_12', 'keep_13', 'keep_14', 'keep_15',
            'keep_16', 'keep_17', 'keep_18', 'keep_19', 'keep_20', 'keep_21', 'keep_22', 'keep_23',
            'keep_24', 'keep_25', 'keep_26', 'keep_27', 'keep_28', 'keep_29', 'keep_30', 'keep_31'
        ), '', s),
        support_phrase_search = 1);

ALTER TABLE text_index_flush_memory_not_in
    MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, max_block_size = 4096;

SYSTEM FLUSH LOGS part_log;

SELECT 'not_in_seed_memory_limit_flushed', ProfileEvents['TextIndexTemporarySegmentsWritten'] > 1
FROM system.part_log
WHERE event_date >= yesterday()
    AND event_time >= now() - 600
    AND database = currentDatabase()
    AND table = 'text_index_flush_memory_not_in'
    AND event_type = 'MutatePart'
    AND error = 0
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE text_index_flush_memory_not_in;
