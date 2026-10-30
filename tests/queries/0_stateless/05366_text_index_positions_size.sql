SET enable_full_text_index = 1;

DROP TABLE IF EXISTS tab_src;
DROP TABLE IF EXISTS tab_plain;
DROP TABLE IF EXISTS tab_phrase;

CREATE TABLE tab_src
(
    id UInt64,
    str String
)
ENGINE = MergeTree
ORDER BY id;

-- Token tN fills about 2^N / 2^20 of the slots, so frequent tokens repeat within a row.
INSERT INTO tab_src SELECT number, arrayStringConcat(arrayMap(i -> concat('t', toString(toUInt8(log2(1 + cityHash64(number, i) % 1048576)))), range(12)), ' ')
FROM numbers(100000);

-- No token repeats within a row.
INSERT INTO tab_src SELECT 100000 + number, arrayStringConcat(arrayMap(i -> concat('u', toString(i), 'x', toString(cityHash64(number, i) % 50)), range(12)), ' ')
FROM numbers(100000);

CREATE TABLE tab_plain
(
    id UInt64,
    str String,
    INDEX idx str TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'pfor', posting_list_block_size = 1048576)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS default_compression_codec = 'LZ4';

CREATE TABLE tab_phrase
(
    id UInt64,
    str String,
    INDEX idx str TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'pfor', posting_list_block_size = 1048576, support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS default_compression_codec = 'LZ4', allow_experimental_text_index_phrase_search = 1;

SELECT 'Repeating tokens: under 0.7 bytes per position';

INSERT INTO tab_plain SELECT * FROM tab_src WHERE id < 100000;
INSERT INTO tab_phrase SELECT * FROM tab_src WHERE id < 100000;
OPTIMIZE TABLE tab_plain FINAL;
OPTIMIZE TABLE tab_phrase FINAL;

WITH
    (SELECT data_compressed_bytes FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab_phrase') AS phrase_bytes,
    (SELECT data_compressed_bytes FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab_plain') AS plain_bytes
SELECT ((phrase_bytes - plain_bytes) / 1200000) < 0.7;

SELECT 'Single positions: under 0.05 bytes per position';

TRUNCATE TABLE tab_plain;
TRUNCATE TABLE tab_phrase;
INSERT INTO tab_plain SELECT * FROM tab_src WHERE id >= 100000;
INSERT INTO tab_phrase SELECT * FROM tab_src WHERE id >= 100000;
OPTIMIZE TABLE tab_plain FINAL;
OPTIMIZE TABLE tab_phrase FINAL;

WITH
    (SELECT data_compressed_bytes FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab_phrase') AS phrase_bytes,
    (SELECT data_compressed_bytes FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab_plain') AS plain_bytes
SELECT ((phrase_bytes - plain_bytes) / 1200000) < 0.05;

DROP TABLE tab_src;
DROP TABLE tab_plain;
DROP TABLE tab_phrase;
