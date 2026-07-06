-- Tags: no-random-settings
-- no-random-settings: pins query_plan_direct_read_from_text_index explicitly below

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106460
-- When a queried part has patch parts (lightweight updates), direct reading from a text index
-- must be disabled. But the tokenizer/preprocessor rewrite of hasToken/hasPhrase must STILL be
-- applied, otherwise the regular (non-direct) fallback returns wrong results for indexes that
-- use a preprocessor (e.g. lower(text)) or a non-default tokenizer (e.g. splitByString). The
-- counts below must be identical with and without a patch part, for both
-- query_plan_direct_read_from_text_index = 0 and = 1.

SET allow_experimental_full_text_index = 1;
SET enable_lightweight_update = 1;
SET use_query_condition_cache = 0;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS t_lwu_text_prep;

CREATE TABLE t_lwu_text_prep
(
    id Int64,
    other Int64,
    text String,
    INDEX idx_text text TYPE text(tokenizer = splitByString([' ', '::']), preprocessor = lower(text))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

-- After lower() + splitByString([' ', '::']):
--   1: 'FoO::Bar' -> ['foo', 'bar']
--   2: 'BAr foO'  -> ['bar', 'foo']
--   3: 'baz qux'  -> ['baz', 'qux']
INSERT INTO t_lwu_text_prep VALUES (1, 0, 'FoO::Bar'), (2, 0, 'BAr foO'), (3, 0, 'baz qux');

SELECT 'no patch, direct=0';
SELECT count() FROM t_lwu_text_prep WHERE hasToken(text, 'FOo') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM t_lwu_text_prep WHERE hasPhrase(text, 'BAR FOO') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'no patch, direct=1';
SELECT count() FROM t_lwu_text_prep WHERE hasToken(text, 'FOo') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM t_lwu_text_prep WHERE hasPhrase(text, 'BAR FOO') SETTINGS query_plan_direct_read_from_text_index = 1;

-- Patch part on `other` (unrelated to the indexed `text` column) forces the fallback path.
UPDATE t_lwu_text_prep SET other = 1 WHERE id = 1;

-- With a patch part direct read is disabled, but the preprocessor/tokenizer rewrite must still
-- run so the fallback returns the same counts as direct=0 above.
SELECT 'patched, direct=0';
SELECT count() FROM t_lwu_text_prep WHERE hasToken(text, 'FOo') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM t_lwu_text_prep WHERE hasPhrase(text, 'BAR FOO') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'patched, direct=1';
SELECT count() FROM t_lwu_text_prep WHERE hasToken(text, 'FOo') SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT count() FROM t_lwu_text_prep WHERE hasPhrase(text, 'BAR FOO') SETTINGS query_plan_direct_read_from_text_index = 1;

DROP TABLE t_lwu_text_prep;
