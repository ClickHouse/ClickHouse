-- Tags: no-random-settings, no-random-merge-tree-settings
DROP TABLE IF EXISTS bloom_sliced_variable_hashes;
SET allow_experimental_bloom_sliced_index = 1;

SELECT '-- variable hash syntax and correctness';

CREATE TABLE bloom_sliced_variable_hashes
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 256, hashes = 4, min_hashes = 1, rows_per_signature = 1) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 1;

INSERT INTO bloom_sliced_variable_hashes
SELECT
    number,
    multiIf(number = 7, 'rare needle common', number % 2 = 0, 'common even', 'common odd')
FROM numbers(32);

SELECT type, granularity, expr
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'bloom_sliced_variable_hashes' AND name = 'idx';

SELECT count() FROM bloom_sliced_variable_hashes WHERE hasToken(text, 'needle') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM bloom_sliced_variable_hashes WHERE hasAllTokens(text, 'rare needle') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM bloom_sliced_variable_hashes WHERE hasToken(text, 'common') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM bloom_sliced_variable_hashes WHERE hasToken(text, 'missing') SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE bloom_sliced_variable_hashes;

SELECT '-- default frequency-conscious hashes';

CREATE TABLE bloom_sliced_default_hashes
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 256, hashes = 4, rows_per_signature = 1) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 1;

INSERT INTO bloom_sliced_default_hashes
SELECT
    number,
    multiIf(number = 7, 'rare needle common', number % 2 = 0, 'common even', 'common odd')
FROM numbers(32);

SELECT count() FROM bloom_sliced_default_hashes WHERE hasToken(text, 'needle') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM bloom_sliced_default_hashes WHERE hasToken(text, 'common') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM bloom_sliced_default_hashes WHERE hasToken(text, 'missing') SETTINGS force_data_skipping_indices = 'idx';
SELECT count()
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_default_hashes WHERE hasToken(text, 'needle') SETTINGS force_data_skipping_indices = 'idx')
WHERE explain LIKE '%Granules: 1/32%';

DROP TABLE bloom_sliced_default_hashes;

SELECT '-- negative: min_hashes must not exceed hashes';

CREATE TABLE bloom_sliced_variable_hashes_bad_arg
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(bits = 256, hashes = 2, min_hashes = 3) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }

SELECT '-- false-positive-rate inference';
DROP TABLE IF EXISTS bloom_sliced_fpr_inference;
CREATE TABLE bloom_sliced_fpr_inference
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), false_positive_rate = 0.05, rows_per_signature = 2) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO bloom_sliced_fpr_inference VALUES
    (1, 'rare needle'), (2, 'common alpha'), (3, 'common beta'), (4, 'common gamma');
SELECT count() FROM bloom_sliced_fpr_inference WHERE hasToken(text, 'needle') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM bloom_sliced_fpr_inference WHERE hasToken(text, 'missing') SETTINGS force_data_skipping_indices = 'idx';
DROP TABLE bloom_sliced_fpr_inference;

SELECT '-- negative: invalid false_positive_rate';
CREATE TABLE bloom_sliced_bad_fpr
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(false_positive_rate = 1.0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }
