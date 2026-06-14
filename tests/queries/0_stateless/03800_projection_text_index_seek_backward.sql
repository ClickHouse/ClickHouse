SET allow_experimental_projection_text_index = 1;
-- Regression test: PostingListCursor::seek arithmetic-mode fast path must
-- not accept targets below `arithmetic_first`.
--
-- Reproduces the race where multi-threaded reads of a single part call
-- AndCursor on the same leaf cursor with non-monotonic row offsets. After
-- one thread advances the cursor to a later packed block
-- (arithmetic_first = 257), another thread issues `seek(target < 257)`. The
-- fast path used to clamp the index to 0 inside seekInArithmeticBlock,
-- leaving the cursor parked at `arithmetic_first` and reporting a
-- non-match for every dropped doc_id — silently shrinking the
-- hasAllTokens result by multiples of `max_block_size`.
--
-- With 8192 dense rows + small max_block_size + small min_rows_for_concurrent_read,
-- the cursor sees many backward seeks across packed blocks and the bug is
-- reproducible at ~100% without the fix.

SET enable_analyzer = 1;
SET enable_full_text_index = 1;
SET max_block_size = 4;
SET max_threads = 32;
SET merge_tree_min_rows_for_concurrent_read = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab_seek_backward;

CREATE TABLE tab_seek_backward
(
    id UInt64,
    arr Array(String),
    PROJECTION idx INDEX arr TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES tab_seek_backward;

-- Single dense part: every row has both 'foo' and 'bar'. With density=1.0
-- on both posting lists, the all-leaf AndCursor path drives through foo
-- and seek-verifies bar; backward seeks across the 256-element TurboPFor
-- packed-block boundary trigger the fast-path clamping bug.
INSERT INTO tab_seek_backward SELECT number, ['foo', 'bar'] FROM numbers(8192);

-- AND result must equal the total row count (every row matches).
SELECT count() FROM tab_seek_backward WHERE hasAllTokens(arr, 'foo bar');

DROP TABLE tab_seek_backward;
