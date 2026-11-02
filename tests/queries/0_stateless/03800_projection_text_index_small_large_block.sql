SET allow_experimental_projection_text_index = 1;
-- Test with small posting_list_block_size to force multiple large blocks per token.
-- With block_size=256, each large block holds exactly 1 packed block (256 docs).
-- Inserting 2000 docs with a common token creates multiple large blocks.

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t_small_large_block;

CREATE TABLE t_small_large_block
(
    id UInt32,
    text String,
    PROJECTION idx_text INDEX text TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 256)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0;

-- Insert 2000 rows, all containing 'common'. With block_size=256, this creates
-- multiple large blocks. Some rows also contain unique tokens.
INSERT INTO t_small_large_block
SELECT
    number,
    'common ' || toString(number) || if(number % 500 = 0, ' rare', '')
FROM numbers(2000);

-- Before merge: verify search across multiple large blocks.
SELECT count() FROM t_small_large_block WHERE hasToken(text, 'common');
SELECT count() FROM t_small_large_block WHERE hasToken(text, 'rare');
SELECT id FROM t_small_large_block WHERE hasToken(text, 'rare') ORDER BY id;

-- Force merge to test merge path with multiple large blocks.
OPTIMIZE TABLE t_small_large_block FINAL;

-- After merge: verify correctness is preserved.
SELECT count() FROM t_small_large_block WHERE hasToken(text, 'common');
SELECT count() FROM t_small_large_block WHERE hasToken(text, 'rare');
SELECT id FROM t_small_large_block WHERE hasToken(text, 'rare') ORDER BY id;

DROP TABLE t_small_large_block;
