SET allow_experimental_projection_text_index = 1;
-- Regression test: sequential stream optimization with contiguous blocks.
-- When iterateLargeBlock skips a contiguous block (detected from metadata, no stream read)
-- at the end of a fill chunk, last_sequential_block must not record it, otherwise the next
-- chunk skips a necessary seek and decodes from the wrong stream position.
-- The bug manifests as hasAllTokens false negatives under small max_block_size.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET max_block_size = 128;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab_seq_stream;

CREATE TABLE tab_seq_stream
(
    id UInt64,
    arr Array(String),
    PROJECTION idx INDEX arr TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1;

SYSTEM STOP MERGES tab_seq_stream;

-- 6 parts with different token combinations
INSERT INTO tab_seq_stream SELECT number, ['abc'] FROM numbers(512);
INSERT INTO tab_seq_stream SELECT number, ['foo'] FROM numbers(512);
INSERT INTO tab_seq_stream SELECT number, ['bar'] FROM numbers(512);
INSERT INTO tab_seq_stream SELECT number, ['foo', 'bar'] FROM numbers(512);
INSERT INTO tab_seq_stream SELECT number, ['foo', 'baz'] FROM numbers(512);
INSERT INTO tab_seq_stream SELECT number, ['bar', 'baz'] FROM numbers(512);

-- Single token queries (baseline)
SELECT 'single_foo', count() FROM tab_seq_stream WHERE hasAllTokens(arr, 'foo');
SELECT 'single_bar', count() FROM tab_seq_stream WHERE hasAllTokens(arr, 'bar');

-- Multi-token AND: only part 4 (['foo', 'bar']) should match
SELECT 'both_foo_bar', count() FROM tab_seq_stream WHERE hasAllTokens(arr, 'foo bar');

-- Verify against brute force
SELECT 'brute_foo_bar', count() FROM tab_seq_stream WHERE hasAllTokens(arr, 'foo bar')
SETTINGS query_plan_direct_read_from_text_index = 0, use_skip_indexes = 0;

DROP TABLE tab_seq_stream;
