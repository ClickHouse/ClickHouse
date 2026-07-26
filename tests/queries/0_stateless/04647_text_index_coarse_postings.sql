-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET use_skip_indexes = 1;

DROP TABLE IF EXISTS tab_coarse;

-- 8192 rows, coarse_granularity = 256 => coarsening budget = 8192/256 = 32 buckets per token, level cap log2(256) = 8.
-- Tokens: 'common' (all rows), 'w0'/'w1'/'w2' (every 3rd row) - coarsen to the maximum level;
-- 'clustered' (100 contiguous rows) - coarsens to a sub-granule level;
-- 'rare' (25 rows) - stays exact.
CREATE TABLE tab_coarse (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, coarse_granularity = 256) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_coarse_granularity = 1, index_granularity = 256, index_granularity_bytes = '10Mi';

INSERT INTO tab_coarse
SELECT number,
    concat('common w', toString(number % 3),
        if(number % 331 = 11, ' rare', ''),
        if(number >= 2980 AND number < 3080, ' clustered', ''))
FROM numbers(8192);

SELECT 'Ground truth (no index)';
SET use_skip_indexes = 0;
SELECT 'common', count() FROM tab_coarse WHERE hasToken(s, 'common');
SELECT 'rare', count(), sum(id) FROM tab_coarse WHERE hasToken(s, 'rare');
SELECT 'clustered', count(), sum(id) FROM tab_coarse WHERE hasToken(s, 'clustered');
SELECT 'all common+rare', count() FROM tab_coarse WHERE hasAllTokens(s, ['common', 'rare']);
SELECT 'all rare+clustered', count(), sum(id) FROM tab_coarse WHERE hasAllTokens(s, ['rare', 'clustered']);
SELECT 'any rare+clustered', count() FROM tab_coarse WHERE hasAnyTokens(s, ['rare', 'clustered']);
SELECT 'rare AND w0', count(), sum(id) FROM tab_coarse WHERE hasToken(s, 'rare') AND hasToken(s, 'w0');
SELECT 'like clustered', count() FROM tab_coarse WHERE s LIKE '%clustered%';
SELECT 'missing', count() FROM tab_coarse WHERE hasToken(s, 'nonexistent');
SET use_skip_indexes = 1;

SELECT 'Direct read, materialize, skip indexes on data read';
SET use_skip_indexes_on_data_read = 1;
SET text_index_posting_list_apply_mode = 'materialize';
SELECT 'common', count() FROM tab_coarse WHERE hasToken(s, 'common');
SELECT 'rare', count(), sum(id) FROM tab_coarse WHERE hasToken(s, 'rare');
SELECT 'clustered', count(), sum(id) FROM tab_coarse WHERE hasToken(s, 'clustered');
SELECT 'all common+rare', count() FROM tab_coarse WHERE hasAllTokens(s, ['common', 'rare']);
SELECT 'all rare+clustered', count(), sum(id) FROM tab_coarse WHERE hasAllTokens(s, ['rare', 'clustered']);
SELECT 'any rare+clustered', count() FROM tab_coarse WHERE hasAnyTokens(s, ['rare', 'clustered']);
SELECT 'rare AND w0', count(), sum(id) FROM tab_coarse WHERE hasToken(s, 'rare') AND hasToken(s, 'w0');
SELECT 'like clustered', count() FROM tab_coarse WHERE s LIKE '%clustered%';
SELECT 'missing', count() FROM tab_coarse WHERE hasToken(s, 'nonexistent');

SELECT 'Direct read, lazy, skip indexes on data read';
SET text_index_posting_list_apply_mode = 'lazy';
SELECT 'common', count() FROM tab_coarse WHERE hasToken(s, 'common');
SELECT 'rare', count(), sum(id) FROM tab_coarse WHERE hasToken(s, 'rare');
SELECT 'clustered', count(), sum(id) FROM tab_coarse WHERE hasToken(s, 'clustered');
SELECT 'all common+rare', count() FROM tab_coarse WHERE hasAllTokens(s, ['common', 'rare']);
SELECT 'all rare+clustered', count(), sum(id) FROM tab_coarse WHERE hasAllTokens(s, ['rare', 'clustered']);
SELECT 'any rare+clustered', count() FROM tab_coarse WHERE hasAnyTokens(s, ['rare', 'clustered']);
SELECT 'rare AND w0', count(), sum(id) FROM tab_coarse WHERE hasToken(s, 'rare') AND hasToken(s, 'w0');
SELECT 'like clustered', count() FROM tab_coarse WHERE s LIKE '%clustered%';
SELECT 'missing', count() FROM tab_coarse WHERE hasToken(s, 'nonexistent');

SELECT 'Direct read, materialize, no skip indexes on data read';
SET use_skip_indexes_on_data_read = 0;
SET text_index_posting_list_apply_mode = 'materialize';
SELECT 'common', count() FROM tab_coarse WHERE hasToken(s, 'common');
SELECT 'rare', count(), sum(id) FROM tab_coarse WHERE hasToken(s, 'rare');
SELECT 'clustered', count(), sum(id) FROM tab_coarse WHERE hasToken(s, 'clustered');
SELECT 'all common+rare', count() FROM tab_coarse WHERE hasAllTokens(s, ['common', 'rare']);
SELECT 'all rare+clustered', count(), sum(id) FROM tab_coarse WHERE hasAllTokens(s, ['rare', 'clustered']);
SELECT 'any rare+clustered', count() FROM tab_coarse WHERE hasAnyTokens(s, ['rare', 'clustered']);
SELECT 'rare AND w0', count(), sum(id) FROM tab_coarse WHERE hasToken(s, 'rare') AND hasToken(s, 'w0');
SELECT 'like clustered', count() FROM tab_coarse WHERE s LIKE '%clustered%';
SELECT 'missing', count() FROM tab_coarse WHERE hasToken(s, 'nonexistent');

SELECT 'Granule pruning only (no direct read)';
SET query_plan_direct_read_from_text_index = 0;
SELECT 'common', count() FROM tab_coarse WHERE hasToken(s, 'common');
SELECT 'rare', count(), sum(id) FROM tab_coarse WHERE hasToken(s, 'rare');
SELECT 'clustered', count(), sum(id) FROM tab_coarse WHERE hasToken(s, 'clustered');
SELECT 'all common+rare', count() FROM tab_coarse WHERE hasAllTokens(s, ['common', 'rare']);
SELECT 'all rare+clustered', count(), sum(id) FROM tab_coarse WHERE hasAllTokens(s, ['rare', 'clustered']);
SELECT 'any rare+clustered', count() FROM tab_coarse WHERE hasAnyTokens(s, ['rare', 'clustered']);
SELECT 'rare AND w0', count(), sum(id) FROM tab_coarse WHERE hasToken(s, 'rare') AND hasToken(s, 'w0');
SELECT 'like clustered', count() FROM tab_coarse WHERE s LIKE '%clustered%';
SELECT 'missing', count() FROM tab_coarse WHERE hasToken(s, 'nonexistent');
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE tab_coarse;

SELECT 'Multi-block coarse posting lists (compressed, small block size)';

DROP TABLE IF EXISTS tab_coarse_blocks;

-- posting_list_block_size = 8 forces coarse posting lists (up to 32 buckets) into multiple
-- compressed segments, exercising the coarse mode of the lazy posting list cursor.
CREATE TABLE tab_coarse_blocks (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, coarse_granularity = 256, posting_list_codec = 'bitpacking', posting_list_block_size = 8) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_coarse_granularity = 1, index_granularity = 256, index_granularity_bytes = '10Mi';

INSERT INTO tab_coarse_blocks
SELECT number,
    concat('common w', toString(number % 3),
        if(number % 331 = 11, ' rare', ''),
        if(number >= 2980 AND number < 3080, ' clustered', ''))
FROM numbers(8192);

SET use_skip_indexes_on_data_read = 1;
SET text_index_posting_list_apply_mode = 'lazy';
SELECT 'common', count() FROM tab_coarse_blocks WHERE hasToken(s, 'common');
SELECT 'rare', count(), sum(id) FROM tab_coarse_blocks WHERE hasToken(s, 'rare');
SELECT 'clustered', count(), sum(id) FROM tab_coarse_blocks WHERE hasToken(s, 'clustered');
SELECT 'all common+rare', count() FROM tab_coarse_blocks WHERE hasAllTokens(s, ['common', 'rare']);
SELECT 'all rare+clustered', count(), sum(id) FROM tab_coarse_blocks WHERE hasAllTokens(s, ['rare', 'clustered']);
SELECT 'any rare+clustered', count() FROM tab_coarse_blocks WHERE hasAnyTokens(s, ['rare', 'clustered']);
SELECT 'rare AND w0', count(), sum(id) FROM tab_coarse_blocks WHERE hasToken(s, 'rare') AND hasToken(s, 'w0');
SELECT 'like clustered', count() FROM tab_coarse_blocks WHERE s LIKE '%clustered%';
SELECT 'missing', count() FROM tab_coarse_blocks WHERE hasToken(s, 'nonexistent');

SET text_index_posting_list_apply_mode = 'materialize';
SELECT 'common', count() FROM tab_coarse_blocks WHERE hasToken(s, 'common');
SELECT 'rare', count(), sum(id) FROM tab_coarse_blocks WHERE hasToken(s, 'rare');
SELECT 'clustered', count(), sum(id) FROM tab_coarse_blocks WHERE hasToken(s, 'clustered');
SELECT 'any rare+clustered', count() FROM tab_coarse_blocks WHERE hasAnyTokens(s, ['rare', 'clustered']);

DROP TABLE tab_coarse_blocks;
