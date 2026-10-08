-- Tests text index parameter `dictionary_block_encoding` and MergeTree setting `text_index_dictionary_block_encoding`.

DROP TABLE IF EXISTS tab;

-- Small blocks so that the last block has a single token; the long tokens need wide length columns.
CREATE TABLE tab
(
    id UInt32,
    s_none String,
    s_v1 String,
    s_v2 String,
    INDEX idx_none(s_none) TYPE text(tokenizer = 'splitByNonAlpha', dictionary_block_size = 3, dictionary_block_encoding = 'none'),
    INDEX idx_v1(s_v1) TYPE text(tokenizer = 'splitByNonAlpha', dictionary_block_size = 3, dictionary_block_encoding = 'frontcoding_v1'),
    INDEX idx_v2(s_v2) TYPE text(tokenizer = 'splitByNonAlpha', dictionary_block_size = 3, dictionary_block_encoding = 'frontcoding_v2')
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab SELECT id, s, s, s FROM values('id UInt32, s String',
    (0, 'token1 token10'),
    (1, 'token11 token2'),
    (2, 'abc abd'),
    (3, repeat('x', 300)),
    (4, repeat('x', 299) || 'y'),
    (5, 'zebra'));

SELECT 'layout';

SELECT DISTINCT dictionary_compression FROM mergeTreeTextIndex(currentDatabase(), tab, idx_none);
SELECT DISTINCT dictionary_compression FROM mergeTreeTextIndex(currentDatabase(), tab, idx_v1);
SELECT DISTINCT dictionary_compression FROM mergeTreeTextIndex(currentDatabase(), tab, idx_v2);

-- Long tokens are shown as their length and last character.
SELECT 'dictionary none';
SELECT if(length(token) > 20, concat(toString(length(token)), ' ', right(token, 1)), token)
FROM mergeTreeTextIndex(currentDatabase(), tab, idx_none)
ORDER BY token;

SELECT 'dictionary frontcoding_v1';
SELECT if(length(token) > 20, concat(toString(length(token)), ' ', right(token, 1)), token)
FROM mergeTreeTextIndex(currentDatabase(), tab, idx_v1)
ORDER BY token;

SELECT 'dictionary frontcoding_v2';
SELECT if(length(token) > 20, concat(toString(length(token)), ' ', right(token, 1)), token)
FROM mergeTreeTextIndex(currentDatabase(), tab, idx_v2)
ORDER BY token;

SELECT 'search';

SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(s_v2, 'token10') SETTINGS force_data_skipping_indices = 'idx_v2';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(s_v2, 'abd') SETTINGS force_data_skipping_indices = 'idx_v2';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(s_v2, repeat('x', 299) || 'y') SETTINGS force_data_skipping_indices = 'idx_v2';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(s_v2, 'zebra') SETTINGS force_data_skipping_indices = 'idx_v2';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(s_v2, 'token3') SETTINGS force_data_skipping_indices = 'idx_v2';

DROP TABLE tab;

SELECT 'merge of parts with different layouts';

CREATE TABLE tab
(
    id UInt32,
    s String,
    INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', dictionary_block_size = 2)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS text_index_dictionary_block_encoding = 'frontcoding_v1';

SYSTEM STOP MERGES tab;

-- The first part is written with `frontcoding_v1`, the second one with `frontcoding_v2`.
INSERT INTO tab VALUES (0, 'foo bar baz');
ALTER TABLE tab MODIFY SETTING text_index_dictionary_block_encoding = 'frontcoding_v2';
INSERT INTO tab VALUES (1, 'foo qux quux');

SELECT DISTINCT part_name, dictionary_compression FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY part_name;

-- The merged part is written with the currently configured layout.
SYSTEM START MERGES tab;
OPTIMIZE TABLE tab FINAL;

SELECT DISTINCT part_name, dictionary_compression FROM mergeTreeTextIndex(currentDatabase(), tab, idx) ORDER BY part_name;

SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(s, 'foo') SETTINGS force_data_skipping_indices = 'idx';
SELECT arraySort(groupArray(id)) FROM tab WHERE hasToken(s, 'quux') SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab;

SELECT 'table settings precedence';

-- An explicitly set `text_index_dictionary_block_encoding` wins over the deprecated setting.
CREATE TABLE tab
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS text_index_dictionary_block_frontcoding_compression = 0, text_index_dictionary_block_encoding = 'frontcoding_v2';

INSERT INTO tab VALUES ('foo');
SELECT DISTINCT dictionary_compression FROM mergeTreeTextIndex(currentDatabase(), tab, idx);

DROP TABLE tab;

-- An index argument wins over the table setting, even the deprecated one.
CREATE TABLE tab
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', dictionary_block_frontcoding_compression = 0)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS text_index_dictionary_block_encoding = 'frontcoding_v2';

INSERT INTO tab VALUES ('foo');
SELECT DISTINCT dictionary_compression FROM mergeTreeTextIndex(currentDatabase(), tab, idx);

DROP TABLE tab;

SELECT 'errors';

-- The new and the deprecated arguments cannot be combined.
CREATE TABLE tab
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', dictionary_block_encoding = 'frontcoding_v1', dictionary_block_frontcoding_compression = 1)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', dictionary_block_encoding = 'unknown')
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = 'splitByNonAlpha', dictionary_block_encoding = 1)
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

SELECT 'size';

-- Sorted n-grams share long prefixes and have short suffixes, which is where front coding helps most.
CREATE TABLE tab_none
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = ngrams(4), dictionary_block_encoding = 'none')
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE tab_v1
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = ngrams(4), dictionary_block_encoding = 'frontcoding_v1')
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE tab_v2
(
    s String,
    INDEX idx(s) TYPE text(tokenizer = ngrams(4), dictionary_block_encoding = 'frontcoding_v2')
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO tab_none SELECT hex(cityHash64(number)) FROM numbers(10000);
INSERT INTO tab_v1 SELECT s FROM tab_none;
INSERT INTO tab_v2 SELECT s FROM tab_none;

SELECT 'uncompressed size: none > frontcoding_v1 > frontcoding_v2';
WITH
    (SELECT data_uncompressed_bytes FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab_none') AS size_none,
    (SELECT data_uncompressed_bytes FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab_v1') AS size_v1,
    (SELECT data_uncompressed_bytes FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab_v2') AS size_v2
SELECT
    size_v1 < 0.95 * size_none,
    size_v2 < 0.95 * size_v1;

DROP TABLE tab_none;
DROP TABLE tab_v1;
DROP TABLE tab_v2;
