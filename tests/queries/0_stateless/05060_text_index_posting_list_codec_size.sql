-- Codec and block size are pinned per index: the harness randomises both, and block size 16 hides the gap.

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS tab_src;
DROP TABLE IF EXISTS tab_none;
DROP TABLE IF EXISTS tab_bitpacking;
DROP TABLE IF EXISTS tab_pfordelta;

CREATE TABLE tab_src (
    id UInt64,
    str String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_src SELECT number, concat(
    'dense ',
    'band' || toString(intDiv(number, 512)), ' ',
    'irr' || toString(cityHash64(number) % 64), ' ',
    'skew' || toString(intDiv(cityHash64(number * 7) % 10000, 100)), ' ',
    if(number % 1000 = 0, 'sparse ', ''),
    if(number IN (7, 900000), 'outlier', ''))
FROM numbers(1000000);

CREATE TABLE tab_none (
    id UInt64,
    str String,
    INDEX idx str TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'none', posting_list_block_size = 1048576)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_bitpacking (
    id UInt64,
    str String,
    INDEX idx str TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'bitpacking', posting_list_block_size = 1048576)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE tab_pfordelta (
    id UInt64,
    str String,
    INDEX idx str TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'pfordelta', posting_list_block_size = 1048576)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab_none SELECT * FROM tab_src;
INSERT INTO tab_bitpacking SELECT * FROM tab_src;
INSERT INTO tab_pfordelta SELECT * FROM tab_src;

OPTIMIZE TABLE tab_none FINAL;
OPTIMIZE TABLE tab_bitpacking FINAL;
OPTIMIZE TABLE tab_pfordelta FINAL;

SELECT 'Same data';
SELECT count() FROM tab_none WHERE hasToken(str, 'dense');
SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'dense');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'dense');
SELECT count() FROM tab_none WHERE hasToken(str, 'outlier');
SELECT count() FROM tab_bitpacking WHERE hasToken(str, 'outlier');
SELECT count() FROM tab_pfordelta WHERE hasToken(str, 'outlier');

SELECT 'Index size ordering';
WITH
    (SELECT data_compressed_bytes FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab_none') AS sz_none,
    (SELECT data_compressed_bytes FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab_bitpacking') AS sz_bitpacking,
    (SELECT data_compressed_bytes FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab_pfordelta') AS sz_pfordelta
SELECT
    sz_bitpacking < sz_none,
    sz_pfordelta < sz_bitpacking,
    -- At least 5% better than bitpacking.
    sz_pfordelta < sz_bitpacking * 0.95;

DROP TABLE tab_src;
DROP TABLE tab_none;
DROP TABLE tab_bitpacking;
DROP TABLE tab_pfordelta;
