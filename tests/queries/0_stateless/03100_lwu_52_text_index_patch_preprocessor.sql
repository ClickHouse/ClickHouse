-- Tags: no-random-settings
-- no-random-settings: relies on query_plan_direct_read_from_text_index being enabled by default

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106460
-- Companion to 03100_lwu_51: same bug (direct text-index read is incompatible with patch parts
-- from lightweight updates) but for an index that uses a preprocessor + a non-default tokenizer.
-- Reading a patch-applied column together with the direct-index read dropped rows. The fix falls
-- back to regular (non-direct) index reading, which must still apply the preprocessor/tokenizer
-- rewrite so the results match query_plan_direct_read_from_text_index = 0. The SELECTs below read
-- the patch-applied column `other`, so they reproduce the bug (empty result on the buggy version)
-- and exercise the preprocessor path at the same time.
--
-- The search terms are chosen to match the raw text case-sensitively as well as through the
-- lower() preprocessor, so the answer is the same whether the index is read directly or the raw
-- column is scanned. That keeps the result independent of the read path (otherwise the case-folded
-- index and the case-sensitive scan would disagree and the reference would be non-deterministic).

SET allow_experimental_full_text_index = 1;
SET enable_lightweight_update = 1;
SET use_query_condition_cache = 0;

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
--   1: 'Foo::Bar' -> ['foo', 'bar']
--   2: 'Bar qux'  -> ['bar', 'qux']
--   3: 'baz Foo'  -> ['baz', 'foo']
INSERT INTO t_lwu_text_prep VALUES (1, 0, 'Foo::Bar'), (2, 0, 'Bar qux'), (3, 0, 'baz Foo');

-- Patch part on `other` (unrelated to the indexed `text` column).
UPDATE t_lwu_text_prep SET other = 1 WHERE id = 1;

-- Reading the patch-applied column `other` together with the preprocessor text-index search.
-- Results must be identical for direct = 0 and direct = 1. 'Foo' matches the raw token in rows 1
-- and 3 (case-sensitive) and matches through lower() in the index; the phrase 'Bar qux' matches
-- row 2. On the buggy version direct = 1 returned nothing because the patched column was read.
SELECT 'hasToken, direct=0';
SELECT id, other FROM t_lwu_text_prep WHERE hasToken(text, 'Foo') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasToken, direct=1';
SELECT id, other FROM t_lwu_text_prep WHERE hasToken(text, 'Foo') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT 'hasPhrase, direct=0';
SELECT id, other FROM t_lwu_text_prep WHERE hasPhrase(text, 'Bar qux') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'hasPhrase, direct=1';
SELECT id, other FROM t_lwu_text_prep WHERE hasPhrase(text, 'Bar qux') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 1;

DROP TABLE t_lwu_text_prep;
