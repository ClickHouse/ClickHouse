-- Regression test for a false negative in mapContainsKeyValue over a keyValuePairs text index.
--
-- A (key, value) pair is stored as either the first-occurrence token (is_rest = 0) or a
-- later-occurrence token (is_rest = 1), so mapContainsKeyValue is the union of the two variants.
-- Representing that union as a single two-token FUNCTION_EQUALS made granule pruning (and direct
-- read) treat the partially folded posting list as complete: while one variant's postings were
-- still unread, a mark holding only that variant was wrongly pruned. It must instead be an OR over
-- the two variants (FUNCTION_HAS_ANY_ELEMENTS), so all postings are read and the union is exact.
--
-- The unread-variant condition arises once a variant's posting list spans more than one posting
-- block. Blocks are split on roaring-container boundaries (65536 row ids), so the later-duplicate
-- rows are placed in two containers (around row 0 and row 65536) and text_index_posting_list_block_size
-- is small; this makes the is_rest = 1 posting multi-block cheaply. Before the fix the index returned
-- 11 instead of 19 on the pruning and direct-read paths.

DROP TABLE IF EXISTS t_mb;

CREATE TABLE t_mb (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, text_index_posting_list_block_size = 4;

INSERT INTO t_mb SELECT number, multiIf(
    number < 8, map('dup', 'y', 'dup', 'x'),                             -- ('dup','x') as later-dup, roaring container 0
    number >= 65536 AND number < 65544, map('dup', 'y', 'dup', 'x'),     -- ('dup','x') as later-dup, roaring container 1
    number >= 100 AND number < 103, map('dup', 'x'),                     -- ('dup','x') as first occurrence (embedded posting)
    map('other', 'z'))
    FROM numbers(65544);

SELECT '-- mapContainsKeyValue: index must equal brute-force (19) on the pruning and direct-read paths --';
SELECT count() FROM t_mb WHERE mapContainsKeyValue(m, 'dup', 'x') SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_mb WHERE mapContainsKeyValue(m, 'dup', 'x') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 0;
SELECT count() FROM t_mb WHERE mapContainsKeyValue(m, 'dup', 'x') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

SELECT '-- the absent pair must still match nothing --';
SELECT count() FROM t_mb WHERE mapContainsKeyValue(m, 'dup', 'absent') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1;

DROP TABLE t_mb;
