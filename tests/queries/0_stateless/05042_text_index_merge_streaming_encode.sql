-- Tests the streaming merge of text index posting lists: the k-way merge of interleaved
-- postings on merges with row remapping and the multi-segment merge on index materialization.
-- `merge_max_block_size` is pinned in the merge tables: a randomized tiny value (down to 1)
-- makes the merge of 100k rows do thousands of pipeline iterations and time out under ThreadFuzzer.

SET mutations_sync = 2;

SELECT '-- merge with row remapping, bitpacking codec, positions';

DROP TABLE IF EXISTS tab_bp_merge;

CREATE TABLE tab_bp_merge
(
    id UInt64,
    s String,
    INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha, support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, text_index_posting_list_codec = 'bitpacking', text_index_posting_list_block_size = 256,
         allow_experimental_text_index_phrase_search = 1, merge_max_block_size = 8192;

SYSTEM STOP MERGES tab_bp_merge;

-- Two parts with fully interleaved sort keys (even and odd ids), so that after the merge
-- the remapped postings of shared tokens interleave between the sources.
-- 'common': one posting per row; 'freq<n>': 1000 rows, split into multiple posting list segments;
-- 'mid<n>': 8 rows, 4 per part (raw postings tier); 'evenrare': 5 rows in one part (embedded tier);
-- 'alpha beta' / 'beta alpha': phrase pairs to check the merge of positions.
INSERT INTO tab_bp_merge SELECT
    number * 2 AS id,
    concat('common freq', toString(id % 100), ' mid', toString(id % 12500), if(id % 4 < 2, ' alpha beta', ' beta alpha'), if(id < 10, ' evenrare', ''))
FROM numbers(50000);

INSERT INTO tab_bp_merge SELECT
    number * 2 + 1 AS id,
    concat('common freq', toString(id % 100), ' mid', toString(id % 12500), if(id % 4 < 2, ' alpha beta', ' beta alpha'))
FROM numbers(50000);

SYSTEM START MERGES tab_bp_merge;
OPTIMIZE TABLE tab_bp_merge FINAL;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_bp_merge' AND active;

SELECT count() FROM tab_bp_merge WHERE hasToken(s, 'common') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_bp_merge WHERE hasToken(s, 'common') SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab_bp_merge WHERE hasToken(s, 'freq7') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_bp_merge WHERE hasToken(s, 'freq7') SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab_bp_merge WHERE hasToken(s, 'mid123') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_bp_merge WHERE hasToken(s, 'mid123') SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab_bp_merge WHERE hasToken(s, 'evenrare') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_bp_merge WHERE hasToken(s, 'evenrare') SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab_bp_merge WHERE hasPhrase(s, 'alpha beta') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_bp_merge WHERE hasPhrase(s, 'alpha beta') SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab_bp_merge WHERE hasPhrase(s, 'beta alpha') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_bp_merge WHERE hasPhrase(s, 'beta alpha') SETTINGS use_skip_indexes = 1;

DROP TABLE tab_bp_merge;

SELECT '-- merge with row remapping, none codec';

DROP TABLE IF EXISTS tab_none_merge;

CREATE TABLE tab_none_merge
(
    id UInt64,
    s String,
    INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, text_index_posting_list_codec = 'none', text_index_posting_list_block_size = 256,
         merge_max_block_size = 8192;

SYSTEM STOP MERGES tab_none_merge;

INSERT INTO tab_none_merge SELECT
    number * 2 AS id,
    concat('common freq', toString(id % 100), ' mid', toString(id % 12500))
FROM numbers(50000);

INSERT INTO tab_none_merge SELECT
    number * 2 + 1 AS id,
    concat('common freq', toString(id % 100), ' mid', toString(id % 12500))
FROM numbers(50000);

SYSTEM START MERGES tab_none_merge;
OPTIMIZE TABLE tab_none_merge FINAL;

SELECT count() FROM tab_none_merge WHERE hasToken(s, 'common') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_none_merge WHERE hasToken(s, 'common') SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab_none_merge WHERE hasToken(s, 'freq7') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_none_merge WHERE hasToken(s, 'freq7') SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab_none_merge WHERE hasToken(s, 'mid123') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_none_merge WHERE hasToken(s, 'mid123') SETTINGS use_skip_indexes = 1;

DROP TABLE tab_none_merge;

SELECT '-- index materialization from multiple temporary segments, bitpacking codec';

DROP TABLE IF EXISTS tab_bp_mat_multi;

CREATE TABLE tab_bp_mat_multi
(
    id UInt64,
    s String
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, text_index_posting_list_codec = 'bitpacking', text_index_posting_list_block_size = 256,
         text_index_max_processed_tokens_before_flush = 10000;

INSERT INTO tab_bp_mat_multi SELECT
    number AS id,
    concat('common freq', toString(id % 100), ' mid', toString(id % 2500))
FROM numbers(20000);

ALTER TABLE tab_bp_mat_multi ADD INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE tab_bp_mat_multi MATERIALIZE INDEX idx;

SELECT count() FROM tab_bp_mat_multi WHERE hasToken(s, 'common') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_bp_mat_multi WHERE hasToken(s, 'common') SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab_bp_mat_multi WHERE hasToken(s, 'freq7') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_bp_mat_multi WHERE hasToken(s, 'freq7') SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab_bp_mat_multi WHERE hasToken(s, 'mid123') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_bp_mat_multi WHERE hasToken(s, 'mid123') SETTINGS use_skip_indexes = 1;

DROP TABLE tab_bp_mat_multi;

SELECT '-- index materialization from a single temporary segment, bitpacking codec';

DROP TABLE IF EXISTS tab_bp_mat_single;

CREATE TABLE tab_bp_mat_single
(
    id UInt64,
    s String
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, text_index_posting_list_codec = 'bitpacking', text_index_posting_list_block_size = 256;

INSERT INTO tab_bp_mat_single SELECT
    number AS id,
    concat('common freq', toString(id % 100))
FROM numbers(20000);

ALTER TABLE tab_bp_mat_single ADD INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE tab_bp_mat_single MATERIALIZE INDEX idx;

SELECT count() FROM tab_bp_mat_single WHERE hasToken(s, 'common') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_bp_mat_single WHERE hasToken(s, 'common') SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab_bp_mat_single WHERE hasToken(s, 'freq7') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_bp_mat_single WHERE hasToken(s, 'freq7') SETTINGS use_skip_indexes = 1;

DROP TABLE tab_bp_mat_single;

SELECT '-- index materialization from a single temporary segment, none codec';

DROP TABLE IF EXISTS tab_none_mat_single;

CREATE TABLE tab_none_mat_single
(
    id UInt64,
    s String
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, text_index_posting_list_codec = 'none', text_index_posting_list_block_size = 256;

INSERT INTO tab_none_mat_single SELECT
    number AS id,
    concat('common freq', toString(id % 100))
FROM numbers(20000);

ALTER TABLE tab_none_mat_single ADD INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE tab_none_mat_single MATERIALIZE INDEX idx;

SELECT count() FROM tab_none_mat_single WHERE hasToken(s, 'common') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_none_mat_single WHERE hasToken(s, 'common') SETTINGS use_skip_indexes = 1;
SELECT count() FROM tab_none_mat_single WHERE hasToken(s, 'freq7') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_none_mat_single WHERE hasToken(s, 'freq7') SETTINGS use_skip_indexes = 1;

DROP TABLE tab_none_mat_single;
