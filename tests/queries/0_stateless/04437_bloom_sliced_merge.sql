-- Tags: no-random-settings, no-random-merge-tree-settings
SET allow_experimental_bloom_sliced_index = 1;

DROP TABLE IF EXISTS bloom_sliced_merge;

CREATE TABLE bloom_sliced_merge
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(tokenizer = splitByNonAlpha(), bits = 2048, hashes = 4, rows_per_signature = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 10;

SYSTEM STOP MERGES bloom_sliced_merge;

INSERT INTO bloom_sliced_merge SELECT number, if(number = 5, 'alpha token', 'filler line') FROM numbers(100);
INSERT INTO bloom_sliced_merge SELECT number, if(number = 105, 'alpha token', 'filler line') FROM numbers(100, 100);
INSERT INTO bloom_sliced_merge SELECT number, if(number = 250, 'beta token', 'filler line') FROM numbers(200, 100);

SELECT '-- counts before merge';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'bloom_sliced_merge' AND active;
SELECT count() FROM bloom_sliced_merge WHERE hasToken(text, 'alpha');
SELECT count() FROM bloom_sliced_merge WHERE hasToken(text, 'beta');
SELECT count() FROM bloom_sliced_merge WHERE hasToken(text, 'absent');

SYSTEM START MERGES bloom_sliced_merge;
OPTIMIZE TABLE bloom_sliced_merge FINAL;

SELECT '-- counts after merge';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'bloom_sliced_merge' AND active;
SELECT count() FROM bloom_sliced_merge WHERE hasToken(text, 'alpha');
SELECT count() FROM bloom_sliced_merge WHERE hasToken(text, 'beta');
SELECT count() FROM bloom_sliced_merge WHERE hasToken(text, 'absent');
SELECT id FROM bloom_sliced_merge WHERE hasToken(text, 'alpha') ORDER BY id;

SELECT '-- pruning after merge';
SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_merge WHERE hasToken(text, 'alpha'))
WHERE explain LIKE '%Name: idx%';
SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_merge WHERE hasToken(text, 'alpha'))
WHERE explain LIKE '%Granules: 2/30%';

DROP TABLE bloom_sliced_merge;
