-- Tags: no-random-settings, no-random-merge-tree-settings
DROP TABLE IF EXISTS bloom_sliced_low_cardinality;
SET allow_experimental_bloom_sliced_index = 1;

SELECT '-- low cardinality string';

CREATE TABLE bloom_sliced_low_cardinality
(
    id UInt64,
    text LowCardinality(String),
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 512, hashes = 3, min_hashes = 3, rows_per_signature = 1) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 1;

INSERT INTO bloom_sliced_low_cardinality VALUES
    (1, 'needle alpha'),
    (2, 'common'),
    (3, 'common'),
    (4, 'haystack'),
    (5, 'needle beta'),
    (6, 'common'),
    (7, 'other'),
    (8, 'common');

SELECT count() FROM bloom_sliced_low_cardinality WHERE hasToken(text, 'needle') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM bloom_sliced_low_cardinality WHERE hasAllTokens(text, 'needle beta') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM bloom_sliced_low_cardinality WHERE hasToken(text, 'missing') SETTINGS force_data_skipping_indices = 'idx';

SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_low_cardinality WHERE hasToken(text, 'needle') SETTINGS force_data_skipping_indices = 'idx')
WHERE explain LIKE '%Name: idx%';
SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_low_cardinality WHERE hasToken(text, 'needle') SETTINGS force_data_skipping_indices = 'idx')
WHERE explain LIKE '%Granules: 2/%';

DROP TABLE bloom_sliced_low_cardinality;

DROP TABLE IF EXISTS bloom_sliced_low_cardinality_nullable;

SELECT '-- low cardinality nullable string';

CREATE TABLE bloom_sliced_low_cardinality_nullable
(
    id UInt64,
    text LowCardinality(Nullable(String)),
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 512, hashes = 3, min_hashes = 3, rows_per_signature = 1) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 1;

INSERT INTO bloom_sliced_low_cardinality_nullable VALUES
    (1, NULL),
    (2, 'common'),
    (3, 'nullable token'),
    (4, NULL),
    (5, 'common'),
    (6, 'other');

SELECT count() FROM bloom_sliced_low_cardinality_nullable WHERE hasToken(text, 'nullable') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM bloom_sliced_low_cardinality_nullable WHERE hasToken(text, 'missing') SETTINGS force_data_skipping_indices = 'idx';

SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_low_cardinality_nullable WHERE hasToken(text, 'nullable') SETTINGS force_data_skipping_indices = 'idx')
WHERE explain LIKE '%Name: idx%';
SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_low_cardinality_nullable WHERE hasToken(text, 'nullable') SETTINGS force_data_skipping_indices = 'idx')
WHERE explain LIKE '%Granules: 1/%';

DROP TABLE bloom_sliced_low_cardinality_nullable;
