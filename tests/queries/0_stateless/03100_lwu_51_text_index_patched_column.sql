-- Tags: no-random-settings
-- no-random-settings: lightweight-update behavior is sensitive to randomized merge/part settings

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106460
-- Direct reading from a text index must not be used when a queried part has patch parts
-- (created by lightweight updates). The direct-read step is prepended to the reader chain and
-- cannot anchor patch application (aligned by _part_offset), so combining it with reading a
-- patch-applied column dropped rows (wrong results) or threw UNKNOWN_IDENTIFIER when the
-- indexed column was MATERIALIZED. This happens even when the patched column is unrelated to
-- the indexed column.

SET allow_experimental_full_text_index = 1;
SET enable_lightweight_update = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_lwu_text_patched;

CREATE TABLE t_lwu_text_patched
(
    id Int64,
    text String,
    other String,
    INDEX idx_other other TYPE text(tokenizer = splitByNonAlpha())
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_text_patched VALUES (1, 'aaa', 'performance tuning'), (2, 'bbb', 'unrelated'), (3, 'ccc', 'performance test');

-- Patch part on `text` (unrelated to the text index on `other`).
UPDATE t_lwu_text_patched SET text = 'patched' WHERE id = 1;

-- Reading a patch-applied column (`text`) together with a direct text-index read on `other`.
-- Pin query_plan_direct_read_from_text_index = 1 so the direct-read path is exercised regardless
-- of a future default change (that path is exactly what this test guards).
SELECT id, text FROM t_lwu_text_patched WHERE hasToken(other, 'performance') ORDER BY id SETTINGS query_plan_direct_read_from_text_index = 1;

-- Compound predicate mixing the direct-index read with a patch-applied column.
SELECT count() FROM t_lwu_text_patched WHERE hasToken(other, 'performance') AND text = 'patched' SETTINGS query_plan_direct_read_from_text_index = 1;

DROP TABLE t_lwu_text_patched;

-- Same bug with a MATERIALIZED indexed column (the exact case in the issue): threw UNKNOWN_IDENTIFIER.
DROP TABLE IF EXISTS t_lwu_text_mat;

CREATE TABLE t_lwu_text_mat
(
    id Int64,
    text String,
    src String,
    mat String MATERIALIZED upper(src),
    INDEX idx_text text TYPE text(tokenizer = splitByNonAlpha()),
    INDEX idx_mat mat TYPE text(tokenizer = splitByNonAlpha())
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_text_mat (id, text, src) VALUES (1, 'database is fast', 'PERFORMANCE TUNING'), (2, 'no match here', 'UNRELATED'), (3, 'database stuff', 'PERFORMANCE TEST');

-- Patch part on `text` (which is indexed by idx_text). Search over the MATERIALIZED column `mat`.
UPDATE t_lwu_text_mat SET text = 'database updated' WHERE id = 1;

SELECT count() FROM t_lwu_text_mat WHERE hasToken(text, 'database') AND hasToken(mat, 'PERFORMANCE') SETTINGS query_plan_direct_read_from_text_index = 1;

DROP TABLE t_lwu_text_mat;
