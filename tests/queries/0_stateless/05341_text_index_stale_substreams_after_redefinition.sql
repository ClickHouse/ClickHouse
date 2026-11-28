-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: uses DETACH PARTITION / ATTACH PARTITION

-- A part keeps the doc lengths substream (`.dl`) of a text index with `scoring = 'bm25'` after the index is redefined
-- without scoring. Part cleanup must see that substream, so a mutation that rebuilds the index does not hardlink it
-- into the new part. The parts are Wide, because a mutation of a Compact part rewrites the whole part.

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_ref_bm25;
DROP TABLE IF EXISTS tab_ref_plain;

CREATE TABLE tab_ref_bm25
(
    id UInt32,
    body String,
    INDEX idx body TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, allow_experimental_text_index_scoring = 1;

CREATE TABLE tab_ref_plain
(
    id UInt32,
    body String,
    INDEX idx body TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, allow_experimental_text_index_scoring = 1;

CREATE TABLE tab
(
    id UInt32,
    body String,
    INDEX idx body TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, allow_experimental_text_index_scoring = 1;

INSERT INTO tab_ref_bm25 SELECT number, concat('word', toString(number % 100), ' common') FROM numbers(1000);
INSERT INTO tab_ref_plain SELECT number, concat('word', toString(number % 100), ' common') FROM numbers(1000);
INSERT INTO tab SELECT number, concat('word', toString(number % 100), ' common') FROM numbers(1000);

-- Redefine the index without scoring while the part is detached, so neither `DROP INDEX` nor the new definition touch it.
ALTER TABLE tab DETACH PARTITION tuple();
ALTER TABLE tab DROP INDEX idx;
ALTER TABLE tab ADD INDEX idx body TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking');
ALTER TABLE tab ATTACH PARTITION tuple();

SELECT '-- the attached part holds the doc lengths substream';
SELECT
    (SELECT sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active)
    = (SELECT sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'tab_ref_bm25' AND active);

-- Updating the indexed column rebuilds the index and hardlinks the files that the mutation does not rewrite.
ALTER TABLE tab UPDATE body = concat(body, '') WHERE 1 SETTINGS mutations_sync = 2;

SELECT '-- the rebuilt index has no doc lengths substream';
SELECT
    (SELECT sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active)
    = (SELECT sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'tab_ref_plain' AND active);

CHECK TABLE tab SETTINGS check_query_single_value_result = 1;
SELECT count() FROM tab WHERE hasToken(body, 'word42');

DROP TABLE tab;
DROP TABLE tab_ref_bm25;
DROP TABLE tab_ref_plain;
