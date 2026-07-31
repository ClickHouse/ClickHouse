-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS tab_coarse_merge;

CREATE TABLE tab_coarse_merge (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, coarse_granularity = 256) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_coarse_granularity = 1, index_granularity = 256, index_granularity_bytes = '10Mi';

SYSTEM STOP MERGES tab_coarse_merge;

-- A merge rebuilds a coarse text index from the merged data (coarse posting lists
-- are not merged). Three parts with row counts that are not multiples of the bucket sizes.
INSERT INTO tab_coarse_merge
SELECT number,
    concat('common w', toString(number % 3),
        if(number % 331 = 11, ' rare', ''),
        if(number >= 2980 AND number < 3080, ' clustered', ''))
FROM numbers(3000);

INSERT INTO tab_coarse_merge
SELECT number + 3000,
    concat('common w', toString((number + 3000) % 3),
        if((number + 3000) % 331 = 11, ' rare', ''),
        if(number + 3000 >= 2980 AND number + 3000 < 3080, ' clustered', ''))
FROM numbers(2500);

INSERT INTO tab_coarse_merge
SELECT number + 5500,
    concat('common w', toString((number + 5500) % 3),
        if((number + 5500) % 331 = 11, ' rare', ''),
        if(number + 5500 >= 2980 AND number + 5500 < 3080, ' clustered', ''))
FROM numbers(2692);

SELECT 'Before merge', count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_coarse_merge' AND active;
SELECT 'common', count() FROM tab_coarse_merge WHERE hasToken(s, 'common');
SELECT 'rare', count(), sum(id) FROM tab_coarse_merge WHERE hasToken(s, 'rare');
SELECT 'clustered', count(), sum(id) FROM tab_coarse_merge WHERE hasToken(s, 'clustered');
SELECT 'all rare+clustered', count(), sum(id) FROM tab_coarse_merge WHERE hasAllTokens(s, ['rare', 'clustered']);
SELECT 'any rare+clustered', count() FROM tab_coarse_merge WHERE hasAnyTokens(s, ['rare', 'clustered']);
SELECT 'rare AND w0', count(), sum(id) FROM tab_coarse_merge WHERE hasToken(s, 'rare') AND hasToken(s, 'w0');

SYSTEM START MERGES tab_coarse_merge;
OPTIMIZE TABLE tab_coarse_merge FINAL;

SELECT 'After merge', count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_coarse_merge' AND active;
SELECT 'common', count() FROM tab_coarse_merge WHERE hasToken(s, 'common');
SELECT 'rare', count(), sum(id) FROM tab_coarse_merge WHERE hasToken(s, 'rare');
SELECT 'clustered', count(), sum(id) FROM tab_coarse_merge WHERE hasToken(s, 'clustered');
SELECT 'all rare+clustered', count(), sum(id) FROM tab_coarse_merge WHERE hasAllTokens(s, ['rare', 'clustered']);
SELECT 'any rare+clustered', count() FROM tab_coarse_merge WHERE hasAnyTokens(s, ['rare', 'clustered']);
SELECT 'rare AND w0', count(), sum(id) FROM tab_coarse_merge WHERE hasToken(s, 'rare') AND hasToken(s, 'w0');

-- The rebuilt index looks exactly like an index built over a single part of 8192 rows:
-- 'common' coarsens to the maximum level 8 (32 buckets), 'clustered' (100 contiguous rows)
-- coarsens to a sub-granule level, 'rare' (25 rows) stays exact.
SELECT token, coarse_level, cardinality
FROM mergeTreeTextIndex(currentDatabase(), tab_coarse_merge, idx)
WHERE token IN ('common', 'clustered', 'rare')
ORDER BY token;

-- A small part merged into a large one: the index is rebuilt with the budget of the merged part.
INSERT INTO tab_coarse_merge SELECT number + 8192, 'common extra' FROM numbers(10);
OPTIMIZE TABLE tab_coarse_merge FINAL;

SELECT 'After small merge', count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_coarse_merge' AND active;
SELECT 'common', count() FROM tab_coarse_merge WHERE hasToken(s, 'common');
SELECT 'extra', count(), sum(id) FROM tab_coarse_merge WHERE hasToken(s, 'extra');
SELECT 'rare', count(), sum(id) FROM tab_coarse_merge WHERE hasToken(s, 'rare');
SELECT 'clustered', count(), sum(id) FROM tab_coarse_merge WHERE hasToken(s, 'clustered');

-- 'extra' (10 rows) is exact in the rebuilt index; 'common' is re-coarsened
-- with the budget of the merged part: ceil(8202 / 256) = 33 buckets.
SELECT token, coarse_level, cardinality
FROM mergeTreeTextIndex(currentDatabase(), tab_coarse_merge, idx)
WHERE token IN ('common', 'extra')
ORDER BY token;

-- The coarse index must be smaller than the exact index on the same data.
DROP TABLE IF EXISTS tab_exact_merge;

CREATE TABLE tab_exact_merge (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 256, index_granularity_bytes = '10Mi';

INSERT INTO tab_exact_merge SELECT id, s FROM tab_coarse_merge;
OPTIMIZE TABLE tab_exact_merge FINAL;

SELECT 'Coarse index is smaller',
    (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'tab_coarse_merge' AND active)
  < (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'tab_exact_merge' AND active);

DROP TABLE tab_coarse_merge;
DROP TABLE tab_exact_merge;
