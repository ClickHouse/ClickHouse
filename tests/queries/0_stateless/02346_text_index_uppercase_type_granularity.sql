-- Regression test for an uppercase `TYPE TEXT` skip index.
-- getSecondaryIndexGranularity compared the index type case-sensitively, so a lowercase `text` index
-- had its GRANULARITY normalized to the whole-part default (100000000) while an uppercase `TEXT` kept
-- the literal GRANULARITY the user wrote. A text index is serialized whole-part, so with GRANULARITY 1
-- the mark-cache prewarm threw CANNOT_READ_ALL_DATA (which wedges SharedMergeTree startup) and, worse,
-- queries using the index silently returned fewer rows than a full scan. Case-insensitive matching
-- normalizes uppercase `TEXT` the same as lowercase `text`.

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id UInt64, msg String, INDEX idx_msg msg TYPE TEXT(tokenizer = splitByNonAlpha) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192;

INSERT INTO tab SELECT number, 'tok' || toString(number % 500) || ' common' FROM numbers(100000);
INSERT INTO tab SELECT number, 'tok' || toString(number % 500) || ' common' FROM numbers(100000, 100000);
OPTIMIZE TABLE tab FINAL;

-- Uppercase `TEXT` must get the whole-part granularity default (100000000), same as lowercase `text`.
SELECT granularity FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab';

-- Prewarming the mark cache must not throw on the whole-part text index. Before the fix this threw
-- (marks expected 25, marks read 1): CANNOT_READ_ALL_DATA with compressed marks (.cmrk2), or
-- CORRUPTED_DATA ("Bad size of marks file") with uncompressed marks (.mrk2).
SYSTEM PREWARM MARK CACHE tab;

-- The index must not change the result: the count using the index equals the full-scan count.
SELECT count() FROM tab WHERE hasToken(msg, 'tok42');
SELECT count() FROM tab WHERE hasToken(msg, 'tok42') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;
