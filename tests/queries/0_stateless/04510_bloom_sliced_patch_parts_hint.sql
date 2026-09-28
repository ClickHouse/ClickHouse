-- Tags: no-random-settings, no-random-merge-tree-settings
-- Pins the interaction of the bloom_sliced staged PREWHERE hint with patch parts (lightweight
-- updates). The hint step reads no physical data and cannot anchor patch application (aligned by
-- _part_offset): for a part with patch parts, the patch system columns turn the hint step into a
-- mixed step that is not dispatched to the index reader, the hint virtual column is not produced,
-- and the reader default-fills it with the fail-open literal 1. This is intentional, per-part
-- degradation rather than a whole-query decline (unlike text index direct read, which replaces
-- the predicate and must decline): the original predicate is always kept as a conjunct of the
-- hint, so results stay correct while patch parts exist, even when the patched column is
-- unrelated to the indexed column, and parts without patches keep a live hint.
SET allow_experimental_bloom_sliced_index = 1;
SET enable_lightweight_update = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS bloom_sliced_patch_hint;

CREATE TABLE bloom_sliced_patch_hint
(
    id UInt64,
    text String,
    other String,
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 2048, hashes = 4, rows_per_signature = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 10,
         enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO bloom_sliced_patch_hint
SELECT number, if(number IN (7, 42), 'needle here', 'filler line'), 'orig' FROM numbers(100);

SELECT '-- hint used before the lightweight update';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT id, other FROM bloom_sliced_patch_hint WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%\_\_bloom\_sliced\_idx%';

-- Patch part on `other` (unrelated to the indexed column `text`).
UPDATE bloom_sliced_patch_hint SET other = 'patched' WHERE id = 7;

-- The hint stays in the plan while the patch part is pending; in the patched part it degrades
-- to the fail-open default at read time, so results must match the hint-off ground truth.
SELECT '-- hint still in the plan while the patch part is pending, results correct';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT id, other FROM bloom_sliced_patch_hint WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%\_\_bloom\_sliced\_idx%';
SELECT id, other FROM bloom_sliced_patch_hint WHERE hasToken(text, 'needle') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT id, other FROM bloom_sliced_patch_hint WHERE hasToken(text, 'needle') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0;
SELECT count() FROM bloom_sliced_patch_hint WHERE hasToken(text, 'needle') AND other = 'patched' SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;

-- Explicit PREWHERE takes the prepend path (and(hint, original)) rather than the plan-level
-- filter path; both must stay correct under the fail-open degradation.
SELECT '-- explicit PREWHERE while the patch part is pending, results correct';
SELECT id, other FROM bloom_sliced_patch_hint PREWHERE hasToken(text, 'needle') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1, optimize_move_to_prewhere = 0;

SELECT '-- mark pruning by the index is still active while the patch part is pending';
SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_patch_hint WHERE hasToken(text, 'needle'))
WHERE explain LIKE '%Granules: 2/10%';

ALTER TABLE bloom_sliced_patch_hint APPLY PATCHES SETTINGS mutations_sync = 2;

SELECT '-- hint fully live again after the patch is materialized, results correct';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT id, other FROM bloom_sliced_patch_hint WHERE hasToken(text, 'needle') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%\_\_bloom\_sliced\_idx%';
SELECT id, other FROM bloom_sliced_patch_hint WHERE hasToken(text, 'needle') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;

OPTIMIZE TABLE bloom_sliced_patch_hint FINAL;

SELECT '-- results correct after merge';
SELECT id, other FROM bloom_sliced_patch_hint WHERE hasToken(text, 'needle') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;

DROP TABLE bloom_sliced_patch_hint;

-- Same scenario with a MATERIALIZED indexed column: the default-fill of the hint virtual column
-- must not try to re-evaluate anything over the MATERIALIZED source column (the analogous text
-- direct-read case threw UNKNOWN_IDENTIFIER).
DROP TABLE IF EXISTS bloom_sliced_patch_hint_mat;

CREATE TABLE bloom_sliced_patch_hint_mat
(
    id UInt64,
    text String,
    src String,
    mat String MATERIALIZED upper(src),
    INDEX idx_mat mat TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 2048, hashes = 4, rows_per_signature = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 10,
         enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO bloom_sliced_patch_hint_mat (id, text, src)
SELECT number, 'plain', if(number IN (7, 42), 'needle here', 'filler line') FROM numbers(100);

-- Patch part on `text` (unrelated to the indexed MATERIALIZED column `mat`).
UPDATE bloom_sliced_patch_hint_mat SET text = 'patched' WHERE id = 7;

SELECT '-- MATERIALIZED indexed column: hint still in the plan while the patch part is pending, results correct';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT id, text FROM bloom_sliced_patch_hint_mat WHERE hasToken(mat, 'NEEDLE') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%\_\_bloom\_sliced\_idx%';
SELECT id, text FROM bloom_sliced_patch_hint_mat WHERE hasToken(mat, 'NEEDLE') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT id, text FROM bloom_sliced_patch_hint_mat WHERE hasToken(mat, 'NEEDLE') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0;
SELECT count() FROM bloom_sliced_patch_hint_mat WHERE hasToken(mat, 'NEEDLE') AND text = 'patched' SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;

ALTER TABLE bloom_sliced_patch_hint_mat APPLY PATCHES SETTINGS mutations_sync = 2;

SELECT '-- MATERIALIZED indexed column: hint fully live again after the patch is materialized, results correct';
SELECT count()
FROM (EXPLAIN actions = 1 SELECT id, text FROM bloom_sliced_patch_hint_mat WHERE hasToken(mat, 'NEEDLE') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%\_\_bloom\_sliced\_idx%';
SELECT id, text FROM bloom_sliced_patch_hint_mat WHERE hasToken(mat, 'NEEDLE') ORDER BY id SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;

DROP TABLE bloom_sliced_patch_hint_mat;
