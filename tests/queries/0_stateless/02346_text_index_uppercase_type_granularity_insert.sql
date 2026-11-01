-- Companion to 02346_text_index_uppercase_type_granularity. That test runs OPTIMIZE FINAL, so it only
-- covers the merge write path (a whole-part single mark). This one keeps a single un-merged insert part
-- to exercise the separate per-granule insert write path (MergeTreeDataPartWriterOnDisk): with the
-- uppercase `TEXT` granularity normalized to the whole-part default, the insert writer must also emit a
-- single whole-part mark, so prewarm does not throw and the index does not change the result.
-- (It does not cover the cross-binary upgrade of a part written under the old per-granule granularity --
-- that is a corrupt-data case repaired by dropping and re-adding the index, not reproducible in a
-- single-binary stateless test.)

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id UInt64, msg String, INDEX idx_msg msg TYPE TEXT(tokenizer = splitByNonAlpha) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192;

-- A single multi-granule insert, NOT merged, so the index keeps the per-granule insert layout.
INSERT INTO tab SELECT number, 'tok' || toString(number % 500) || ' common' FROM numbers(100000);

-- Uppercase `TEXT` must get the whole-part granularity default (100000000), same as lowercase `text`.
SELECT granularity FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab';

-- Prewarming the mark cache must not throw on the un-merged insert part.
SYSTEM PREWARM MARK CACHE tab;

-- The index must not change the result: the count using the index equals the full-scan count.
SELECT count() FROM tab WHERE hasToken(msg, 'tok42');
SELECT count() FROM tab WHERE hasToken(msg, 'tok42') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;
