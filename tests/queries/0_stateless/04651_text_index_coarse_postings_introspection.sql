DROP TABLE IF EXISTS tab_coarse_introspect;

-- 8192 rows, coarse_granularity = 256 => coarsening budget = 8192/256 = 32 buckets per token
-- and the maximum coarse level log2(256) = 8.
CREATE TABLE tab_coarse_introspect (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha, coarse_granularity = 256) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_experimental_text_index_coarse_granularity = 1, index_granularity = 256, index_granularity_bytes = '10Mi';

INSERT INTO tab_coarse_introspect
SELECT number,
    concat('common w', toString(number % 3),
        if(number % 331 = 11, ' rare', ''),
        if(number >= 2980 AND number < 3080, ' clustered', ''))
FROM numbers(8192);

-- 'rare' (25 rows) stays exact: coarse_level = 0, cardinality = number of rows.
-- 'clustered' (100 contiguous rows) coarsens to a sub-granule level: 25 buckets of 4 rows.
-- 'common' and 'w0'/'w1'/'w2' are spread over the whole part and coarsen
-- to the maximum level 8: cardinality is the number of stored buckets.
SELECT token, coarse_level, cardinality
FROM mergeTreeTextIndex(currentDatabase(), tab_coarse_introspect, idx)
ORDER BY token;

DROP TABLE tab_coarse_introspect;
