-- Test that the projection text index cannot be created with fixed granularity.

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    `id`   UInt64,
    `text` String,
    PROJECTION inv_idx INDEX text TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 32, index_granularity_bytes = 0, min_bytes_for_wide_part = 0; -- {serverError SUPPORT_IS_DISABLED }
