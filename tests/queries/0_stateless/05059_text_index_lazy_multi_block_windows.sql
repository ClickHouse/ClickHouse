-- The lazy posting-list cursor applies a token's postings to the output column one granule at a time
-- (`linearOr` / `linearAnd`), reusing the cursor across the consecutive per-granule windows of a part.
-- The posting lists below span many packed blocks (128 postings each) and many segments
-- (`posting_list_block_size` postings each), and both block and segment boundaries straddle the
-- 8192-row granule boundaries. Every lazy result is compared with a plain column scan.

SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_optimize_count_from_text_index = 0;
SET use_query_condition_cache = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS tab_lazy_windows;

-- One index granule covers the whole part, so every posting list spans all 25 granules of the part.
CREATE TABLE tab_lazy_windows
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'bitpacking', posting_list_block_size = 1024) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

--   third   : every 3rd row -> 66667 postings, 2730.67 per granule: packed blocks and segments straddle granule boundaries
--   seventh : every 7th row -> 28572 postings
--   even    : every 2nd row -> 100000 postings, exactly 32 packed blocks per granule: block boundaries coincide with granule boundaries
--   rare    : 200 postings  -> embedded (single-block) postings
INSERT INTO tab_lazy_windows
SELECT number,
    concat('base',
        if(number % 3 = 0, ' third', ''),
        if(number % 7 = 0, ' seventh', ''),
        if(number % 2 = 0, ' even', ''),
        if(number % 1000 = 999, ' rare', ''))
FROM numbers(200000)
SETTINGS max_insert_threads = 1, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_lazy_windows' AND active;

SELECT 'Ground truth (no index)';
SET use_skip_indexes = 0;
SELECT 'third', count(), sum(id) FROM tab_lazy_windows WHERE hasToken(s, 'third');
SELECT 'third in [100000, 120000)', count(), sum(id) FROM tab_lazy_windows WHERE id >= 100000 AND id < 120000 AND hasToken(s, 'third');
SELECT 'any third seventh', count(), sum(id) FROM tab_lazy_windows WHERE hasAnyTokens(s, ['third', 'seventh']);
SELECT 'any even rare', count(), sum(id) FROM tab_lazy_windows WHERE hasAnyTokens(s, ['even', 'rare']);
SELECT 'all third seventh', count(), sum(id) FROM tab_lazy_windows WHERE hasAllTokens(s, ['third', 'seventh']);
SELECT 'all even third', count(), sum(id) FROM tab_lazy_windows WHERE hasAllTokens(s, ['even', 'third']);
SELECT 'all third rare', count(), sum(id) FROM tab_lazy_windows WHERE hasAllTokens(s, ['third', 'rare']);
SELECT 'all even third seventh', count(), sum(id) FROM tab_lazy_windows WHERE hasAllTokens(s, ['even', 'third', 'seventh']);

SET use_skip_indexes = 1;
SET text_index_posting_list_apply_mode = 'lazy';

SELECT 'Lazy, leapfrog intersection';
SET text_index_lazy_intersection_density_threshold = 1;
SELECT 'third', count(), sum(id) FROM tab_lazy_windows WHERE hasToken(s, 'third');
SELECT 'third in [100000, 120000)', count(), sum(id) FROM tab_lazy_windows WHERE id >= 100000 AND id < 120000 AND hasToken(s, 'third');
SELECT 'any third seventh', count(), sum(id) FROM tab_lazy_windows WHERE hasAnyTokens(s, ['third', 'seventh']);
SELECT 'any even rare', count(), sum(id) FROM tab_lazy_windows WHERE hasAnyTokens(s, ['even', 'rare']);
SELECT 'all third seventh', count(), sum(id) FROM tab_lazy_windows WHERE hasAllTokens(s, ['third', 'seventh']);
SELECT 'all even third', count(), sum(id) FROM tab_lazy_windows WHERE hasAllTokens(s, ['even', 'third']);
SELECT 'all third rare', count(), sum(id) FROM tab_lazy_windows WHERE hasAllTokens(s, ['third', 'rare']);
SELECT 'all even third seventh', count(), sum(id) FROM tab_lazy_windows WHERE hasAllTokens(s, ['even', 'third', 'seventh']);

SELECT 'Lazy, brute-force intersection';
SET text_index_lazy_intersection_density_threshold = 0;
SELECT 'all third seventh', count(), sum(id) FROM tab_lazy_windows WHERE hasAllTokens(s, ['third', 'seventh']);
SELECT 'all even third', count(), sum(id) FROM tab_lazy_windows WHERE hasAllTokens(s, ['even', 'third']);
SELECT 'all third rare', count(), sum(id) FROM tab_lazy_windows WHERE hasAllTokens(s, ['third', 'rare']);
SELECT 'all even third seventh', count(), sum(id) FROM tab_lazy_windows WHERE hasAllTokens(s, ['even', 'third', 'seventh']);

DROP TABLE tab_lazy_windows;
