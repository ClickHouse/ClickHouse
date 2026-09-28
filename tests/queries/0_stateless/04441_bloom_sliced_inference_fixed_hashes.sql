-- Tags: no-random-settings, no-random-merge-tree-settings
SET allow_experimental_bloom_sliced_index = 1;

-- Inference lands on `hashes == min_hashes`: with a high false-positive-rate target and hex
-- payloads (at most 16^3 distinct trigrams, so thousands of tokens per 8192-row signature) the
-- inferred hash count clamps to 1, which equals the default `min_hashes`. The part must then be
-- written entirely in fixed-hash format and stay readable.
DROP TABLE IF EXISTS bloom_sliced_inference_min_hashes;

CREATE TABLE bloom_sliced_inference_min_hashes
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(tokenizer = ngrams(3), false_positive_rate = 0.9, rows_per_signature = 8192) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 8192;

INSERT INTO bloom_sliced_inference_min_hashes SELECT number, hex(sipHash64(number)) FROM numbers(20000);

SELECT '-- part built with inferred minimum hash count is readable';
SELECT count() FROM bloom_sliced_inference_min_hashes WHERE hasToken(text, hex(sipHash64(toUInt64(7777))));
SELECT count() FROM bloom_sliced_inference_min_hashes WHERE hasToken(text, hex(sipHash64(toUInt64(7777)))) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE bloom_sliced_inference_min_hashes;

-- With explicit `min_hashes` equal to the (default) `hashes`, the build is fixed-hash from the
-- start but false-positive-rate inference must still run and size the signature.
DROP TABLE IF EXISTS bloom_sliced_inference_fixed;

CREATE TABLE bloom_sliced_inference_fixed
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), false_positive_rate = 0.01, min_hashes = 4, rows_per_signature = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 10;

INSERT INTO bloom_sliced_inference_fixed
SELECT number, if(number = 42, 'needle present', 'filler line') FROM numbers(100);

SELECT '-- fixed-hash inference: correct results and pruning';
SELECT count() FROM bloom_sliced_inference_fixed WHERE hasToken(text, 'needle');
SELECT id FROM bloom_sliced_inference_fixed WHERE hasToken(text, 'needle');
SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_inference_fixed WHERE hasToken(text, 'needle'))
WHERE explain LIKE '%Granules: 1/10%';

DROP TABLE bloom_sliced_inference_fixed;
