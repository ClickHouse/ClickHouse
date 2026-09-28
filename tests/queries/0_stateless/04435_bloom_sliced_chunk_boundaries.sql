-- Tags: no-random-settings, no-random-merge-tree-settings
SET allow_experimental_bloom_sliced_index = 1;

DROP TABLE IF EXISTS bloom_sliced_chunk_boundaries;

CREATE TABLE bloom_sliced_chunk_boundaries
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(bits = 16384, hashes = 4, min_hashes = 4, rows_per_signature = 1) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 8192;

INSERT INTO bloom_sliced_chunk_boundaries
SELECT
    number,
    arrayStringConcat(arrayFilter(x -> x != '', [
        'denseall',
        if(number < 1048576, 'firstonly', ''),
        if(number = 1049000, 'secondonly', ''),
        if(number IN (1048575, 1048576), 'spanning', '')
    ]), ' ')
FROM numbers(1049600);

SELECT '-- correctness across chunks';
SELECT count() FROM bloom_sliced_chunk_boundaries WHERE hasToken(text, 'firstonly');
SELECT count() FROM bloom_sliced_chunk_boundaries WHERE hasToken(text, 'secondonly');
SELECT count() FROM bloom_sliced_chunk_boundaries WHERE hasToken(text, 'spanning');
SELECT count() FROM bloom_sliced_chunk_boundaries WHERE id >= 1048576 AND hasToken(text, 'firstonly');
SELECT count() FROM bloom_sliced_chunk_boundaries WHERE id < 1048576 AND hasToken(text, 'secondonly');

SELECT '-- chunk pruning';
SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_chunk_boundaries WHERE id >= 1048576 AND hasToken(text, 'firstonly'))
WHERE explain LIKE '%Name: idx%';
SELECT count() > 0
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_chunk_boundaries WHERE id >= 1048576 AND hasToken(text, 'firstonly'))
WHERE explain LIKE '%Granules: 1/%';

SELECT '-- direct read hint with primary-key range';
SELECT count() FROM bloom_sliced_chunk_boundaries PREWHERE id >= 1048576 WHERE hasToken(text, 'secondonly') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count()
FROM (EXPLAIN actions = 1 SELECT count() FROM bloom_sliced_chunk_boundaries PREWHERE id >= 1048576 WHERE hasToken(text, 'secondonly') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1)
WHERE explain LIKE '%__bloom_sliced_idx%';

SELECT '-- dense first chunk / absent second chunk';
SELECT count() FROM bloom_sliced_chunk_boundaries WHERE id >= 1048576 AND hasToken(text, 'denseall');
SELECT count() FROM bloom_sliced_chunk_boundaries WHERE id >= 1048576 AND hasToken(text, 'firstonly');

DROP TABLE bloom_sliced_chunk_boundaries;
