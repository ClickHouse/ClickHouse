-- Tags: no-random-settings, no-random-merge-tree-settings
-- Tombstone Bloom filters for lossy `bloom_sliced` preprocessors. The preprocessor strips
-- ISO-date prefixes, so date tokens exist in the raw column but not in the stored tokens. The
-- index records the lost raw tokens per chunk in a tombstone Bloom filter: probing a lost token
-- fails the chunk open (correct results instead of false negatives), while chunks that never
-- lost the token and tokens that were never lost keep pruning. Rows below 1048576 (the chunk
-- size) carry a 2026 date, rows above carry a 2027 date, so each date's tokens are tombstoned
-- only in its own chunk.
SET allow_experimental_bloom_sliced_index = 1;

DROP TABLE IF EXISTS bloom_sliced_tombstones;

CREATE TABLE bloom_sliced_tombstones
(
    id UInt64,
    text String,
    INDEX idx text TYPE bloom_sliced(
        tokenizer = splitByNonAlpha,
        preprocessor = replaceRegexpAll(text, '[0-9]{4}-[0-9]{2}-[0-9]{2}', ' '),
        bits = 8192,
        hashes = 4,
        min_hashes = 4,
        rows_per_signature = 256
    ) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 8192;

INSERT INTO bloom_sliced_tombstones
SELECT
    number,
    concat(
        if(number < 1048576, '2026-01-01', '2027-02-02'),
        ' log alpha w', toString(number % 997),
        if(number BETWEEN 5000 AND 5099, ' rare1', ''),
        if(number BETWEEN 1049000 AND 1049099, ' rare2', ''))
FROM numbers(1049600);

SELECT '-- lost tokens fail open and stay correct, hint on and off (ground truth: no skip indexes)';
SELECT count() FROM bloom_sliced_tombstones WHERE hasToken(text, '2026') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count() FROM bloom_sliced_tombstones WHERE hasToken(text, '2026') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0;
SELECT count() FROM bloom_sliced_tombstones WHERE hasToken(text, '2026') SETTINGS use_skip_indexes = 0;
SELECT count() FROM bloom_sliced_tombstones WHERE hasToken(text, '2027') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count() FROM bloom_sliced_tombstones WHERE hasToken(text, '2027') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0;
SELECT count() FROM bloom_sliced_tombstones WHERE hasToken(text, '2027') SETTINGS use_skip_indexes = 0;

SELECT '-- LIKE with a lost interior token, hint on and off';
SELECT count() FROM bloom_sliced_tombstones WHERE text LIKE '%2026-01-01%' SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count() FROM bloom_sliced_tombstones WHERE text LIKE '%2026-01-01%' SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0;
SELECT count() FROM bloom_sliced_tombstones WHERE text LIKE '%2026-01-01%' SETTINGS use_skip_indexes = 0;

SELECT '-- hasAllTokens mixing a lost and a never-lost token, hint on and off';
SELECT count() FROM bloom_sliced_tombstones WHERE hasAllTokens(text, '2026 rare1') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count() FROM bloom_sliced_tombstones WHERE hasAllTokens(text, '2026 rare1') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0;
SELECT count() FROM bloom_sliced_tombstones WHERE hasAllTokens(text, '2026 rare1') SETTINGS use_skip_indexes = 0;

SELECT '-- NOT and OR shapes stay correct';
SELECT count() FROM bloom_sliced_tombstones WHERE NOT hasToken(text, '2026') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count() FROM bloom_sliced_tombstones WHERE hasToken(text, '2026') OR hasToken(text, 'rare2') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT count() FROM bloom_sliced_tombstones WHERE hasToken(text, '2026') OR hasToken(text, 'rare2') SETTINGS use_skip_indexes = 0;

SELECT '-- a token lost only in the first chunk prunes the second chunk (granule counts)';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_tombstones WHERE hasToken(text, '2026') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0)
WHERE explain LIKE '%Granules: %/%' AND explain NOT LIKE '%129/129%';

SELECT '-- a token lost only in the second chunk prunes the first chunk (granule counts)';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_tombstones WHERE hasToken(text, '2027') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0)
WHERE explain LIKE '%Granules: %/%' AND explain NOT LIKE '%129/129%';

SELECT '-- never-lost tokens keep pruning (granule counts)';
SELECT count() FROM bloom_sliced_tombstones WHERE hasToken(text, 'rare1') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_tombstones WHERE hasToken(text, 'rare1') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0)
WHERE explain LIKE '%Granules: %/%' AND explain NOT LIKE '%129/129%';
SELECT count() FROM bloom_sliced_tombstones WHERE hasToken(text, 'rare2') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 1;
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_tombstones WHERE hasToken(text, 'rare2') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0)
WHERE explain LIKE '%Granules: %/%' AND explain NOT LIKE '%129/129%';

SELECT '-- the mixed hasAllTokens keeps pruning from the never-lost token inside the failed-open chunk';
SELECT trimLeft(explain)
FROM (EXPLAIN indexes = 1 SELECT count() FROM bloom_sliced_tombstones WHERE hasAllTokens(text, '2026 rare1') SETTINGS query_plan_direct_read_from_bloom_sliced_index = 0)
WHERE explain LIKE '%Granules: %/%' AND explain NOT LIKE '%129/129%';

DROP TABLE bloom_sliced_tombstones;
