-- Tests that text index layout parameters can be configured at the table level via MergeTree settings
-- (text_index_dictionary_block_frontcoding_compression, text_index_posting_list_block_size and
-- text_index_posting_list_codec), that these table-level defaults feed the index creator, and that an
-- explicit per-index argument still overrides the table setting.
--
-- The defaults of these settings happen to equal the historical built-in constants, so the tables below
-- deliberately pin non-default values; a silent fallback to the old constants would show the default layout.
-- The resulting layout is observed through the mergeTreeTextIndex() introspection function:
--   - dictionary_compression  -> 'raw' (front coding off) vs 'front_coded' (front coding on),
--   - num_posting_blocks      -> how the posting list of a high-cardinality token is split into blocks,
--   - has_compressed_postings -> whether the posting list is stored with the 'bitpacking' codec.
-- The high-cardinality token 'hello' is present in every row; with 200000 rows its posting list spans
-- multiple roaring containers, so a small posting_list_block_size splits it into more than one block.

SET mutations_sync = 2;

-- 1. A text index without per-index layout arguments, under non-default table settings.
DROP TABLE IF EXISTS t_text_settings;
CREATE TABLE t_text_settings (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS
    text_index_dictionary_block_frontcoding_compression = 0, -- default 1 (front coding on)
    text_index_posting_list_block_size = 256,                -- default 1048576 (single block)
    text_index_posting_list_codec = 'bitpacking';            -- default 'none' (uncompressed)

INSERT INTO t_text_settings SELECT 'hello world ' || toString(number) FROM numbers(200000);

SELECT 'table settings feed the index layout';
SELECT DISTINCT dictionary_compression FROM mergeTreeTextIndex(currentDatabase(), t_text_settings, idx);
SELECT num_posting_blocks > 1 FROM mergeTreeTextIndex(currentDatabase(), t_text_settings, idx) WHERE token = 'hello';
SELECT has_compressed_postings FROM mergeTreeTextIndex(currentDatabase(), t_text_settings, idx) WHERE token = 'hello';
SELECT count() FROM t_text_settings WHERE hasToken(s, 'hello');

DROP TABLE t_text_settings;

-- 2. The default table settings (pinned explicitly so test setting randomization does not interfere)
--    produce the opposite, default layout, as a contrast.
DROP TABLE IF EXISTS t_text_defaults;
CREATE TABLE t_text_defaults (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS
    text_index_dictionary_block_frontcoding_compression = 1,
    text_index_posting_list_block_size = 1048576,
    text_index_posting_list_codec = 'none';

INSERT INTO t_text_defaults SELECT 'hello world ' || toString(number) FROM numbers(200000);

SELECT 'default settings produce the default layout';
SELECT DISTINCT dictionary_compression FROM mergeTreeTextIndex(currentDatabase(), t_text_defaults, idx);
SELECT num_posting_blocks > 1 FROM mergeTreeTextIndex(currentDatabase(), t_text_defaults, idx) WHERE token = 'hello';
SELECT has_compressed_postings FROM mergeTreeTextIndex(currentDatabase(), t_text_defaults, idx) WHERE token = 'hello';

DROP TABLE t_text_defaults;

-- 3. An explicit per-index argument overrides the table setting: the index arguments here restore the
--    default layout even though the table settings request the non-default one.
DROP TABLE IF EXISTS t_text_override;
CREATE TABLE t_text_override (s String, INDEX idx s TYPE text(
        tokenizer = 'splitByNonAlpha',
        dictionary_block_frontcoding_compression = 1,
        posting_list_block_size = 1048576,
        posting_list_codec = 'none'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS
    text_index_dictionary_block_frontcoding_compression = 0,
    text_index_posting_list_block_size = 256,
    text_index_posting_list_codec = 'bitpacking';

INSERT INTO t_text_override SELECT 'hello world ' || toString(number) FROM numbers(200000);

SELECT 'index arguments override table settings';
SELECT DISTINCT dictionary_compression FROM mergeTreeTextIndex(currentDatabase(), t_text_override, idx);
SELECT num_posting_blocks > 1 FROM mergeTreeTextIndex(currentDatabase(), t_text_override, idx) WHERE token = 'hello';
SELECT has_compressed_postings FROM mergeTreeTextIndex(currentDatabase(), t_text_override, idx) WHERE token = 'hello';

DROP TABLE t_text_override;
