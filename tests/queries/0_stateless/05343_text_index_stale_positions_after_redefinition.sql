-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: uses DETACH PARTITION / ATTACH PARTITION

-- A part keeps the positions substream (`.pos`) of a text index with `support_phrase_search = 1` after the index is
-- redefined without phrase search. Part cleanup must see that substream, so a mutation that rebuilds the index does
-- not hardlink it into the new part. The parts are Wide, because a mutation of a Compact part rewrites the whole part.

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_ref_positions;
DROP TABLE IF EXISTS tab_ref_plain;

CREATE TABLE tab_ref_positions
(
    id UInt32,
    body String,
    INDEX idx body TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, allow_experimental_text_index_phrase_search = 1;

CREATE TABLE tab_ref_plain
(
    id UInt32,
    body String,
    INDEX idx body TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE tab
(
    id UInt32,
    body String,
    INDEX idx body TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, allow_experimental_text_index_phrase_search = 1;

INSERT INTO tab_ref_positions SELECT number, concat('word', toString(number % 100), ' common') FROM numbers(1000);
INSERT INTO tab_ref_plain SELECT number, concat('word', toString(number % 100), ' common') FROM numbers(1000);
INSERT INTO tab SELECT number, concat('word', toString(number % 100), ' common') FROM numbers(1000);

-- Redefine the index without phrase search while the part is detached, so neither `DROP INDEX` nor the new definition touch it.
ALTER TABLE tab DETACH PARTITION tuple();
ALTER TABLE tab DROP INDEX idx;
ALTER TABLE tab ADD INDEX idx body TYPE text(tokenizer = 'splitByNonAlpha');
ALTER TABLE tab ATTACH PARTITION tuple();

SELECT '-- the attached part holds the positions substream';
SELECT
    (SELECT sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active)
    = (SELECT sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'tab_ref_positions' AND active);

-- Updating the indexed column rebuilds the index and hardlinks the files that the mutation does not rewrite.
ALTER TABLE tab UPDATE body = concat(body, '') WHERE 1 SETTINGS mutations_sync = 2;

SELECT '-- the rebuilt index has no positions substream';
SELECT
    (SELECT sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active)
    = (SELECT sum(secondary_indices_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'tab_ref_plain' AND active);

CHECK TABLE tab SETTINGS check_query_single_value_result = 1;
SELECT count() FROM tab WHERE hasToken(body, 'word42');

DROP TABLE tab;
DROP TABLE tab_ref_positions;
DROP TABLE tab_ref_plain;
