-- Exercises intersectLeapfrogHeap, the >8-cursor variant of the leapfrog AND
-- algorithm. Dispatch in MergeTreeIndexTextPostingListCursor.cpp:
--   n == 2 -> intersectTwo
--   n == 3 -> intersectThree
--   n == 4 -> intersectFour
--   n <= 8 -> intersectLeapfrogLinear
--   n  > 8 -> intersectLeapfrogHeap        <- this file
-- Unit tests cover the heap variant up to 10 cursors, but no SQL test exercises
-- it through the production reader path. This file fills that gap.

SET enable_full_text_index = 1;
SET allow_experimental_text_index_lazy_apply = 1;
SET text_index_posting_list_apply_mode = 'lazy';
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab_heap;

-- posting_list_block_size = 256 keeps non-embedded tokens on the multi-segment
-- cursor path (cardinality > 256 -> not SingleBlock).
CREATE TABLE tab_heap(
    k UInt64,
    s String,
    INDEX idx s TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking',
        posting_list_block_size = 256))
ENGINE = MergeTree() ORDER BY k
SETTINGS index_granularity = 8192;

-- Each token tk_i is in rows divisible by (i + 2), for i = 0..10.
--   tka: number % 2  = 0  -> 25000 docs (density 0.5)
--   tkb: number % 3  = 0  -> 16667
--   tkc: number % 4  = 0  -> 12500
--   tkd: number % 5  = 0  -> 10000
--   tke: number % 6  = 0  ->  8334
--   tkf: number % 7  = 0  ->  7143
--   tkg: number % 8  = 0  ->  6250
--   tkh: number % 9  = 0  ->  5556
--   tki: number % 10 = 0  ->  5000
--   tkj: number % 11 = 0  ->  4546
--   tkk: number % 13 = 0  ->  3847
-- All cardinalities exceed posting_list_block_size (256), so every token stays on
-- the lazy multi-segment cursor path (no SingleBlock preload).
INSERT INTO tab_heap
SELECT number,
    concat(
        if(number % 2  = 0, 'tka ', ''),
        if(number % 3  = 0, 'tkb ', ''),
        if(number % 4  = 0, 'tkc ', ''),
        if(number % 5  = 0, 'tkd ', ''),
        if(number % 6  = 0, 'tke ', ''),
        if(number % 7  = 0, 'tkf ', ''),
        if(number % 8  = 0, 'tkg ', ''),
        if(number % 9  = 0, 'tkh ', ''),
        if(number % 10 = 0, 'tki ', ''),
        if(number % 11 = 0, 'tkj ', ''),
        if(number % 13 = 0, 'tkk', '')
    )
FROM numbers(50000);

-- Sanity-check cardinalities. All must be > 256 (posting_list_block_size cap) so
-- every cursor stays on the lazy multi-segment path rather than the embedded
-- rare-token preload.
SELECT 'cardinality tka', count() FROM tab_heap WHERE hasToken(s, 'tka');
SELECT 'cardinality tkb', count() FROM tab_heap WHERE hasToken(s, 'tkb');
SELECT 'cardinality tkc', count() FROM tab_heap WHERE hasToken(s, 'tkc');
SELECT 'cardinality tkd', count() FROM tab_heap WHERE hasToken(s, 'tkd');
SELECT 'cardinality tke', count() FROM tab_heap WHERE hasToken(s, 'tke');
SELECT 'cardinality tkf', count() FROM tab_heap WHERE hasToken(s, 'tkf');
SELECT 'cardinality tkg', count() FROM tab_heap WHERE hasToken(s, 'tkg');
SELECT 'cardinality tkh', count() FROM tab_heap WHERE hasToken(s, 'tkh');
SELECT 'cardinality tki', count() FROM tab_heap WHERE hasToken(s, 'tki');
SELECT 'cardinality tkj', count() FROM tab_heap WHERE hasToken(s, 'tkj');
SELECT 'cardinality tkk', count() FROM tab_heap WHERE hasToken(s, 'tkk');

-- 9-way AND just past the dispatch boundary (n == 9 -> heap).
-- LCM(2..10) = 2520 -> floor(50000 / 2520) + 1 = 20 matches in [0, 50000).
-- Force leapfrog with text_index_density_threshold = 1.0 (min_density = 0.1 < 1.0).
SELECT 'and 9-way:', count() FROM tab_heap
WHERE hasAllTokens(s, ['tka', 'tkb', 'tkc', 'tkd', 'tke', 'tkf', 'tkg', 'tkh', 'tki'])
SETTINGS text_index_density_threshold = 1.0,
         log_comment = '04258_heap_9way';

-- Verify the exact matching row IDs (catches off-by-one regressions in the heap
-- loop). Rows divisible by 2520 in [0, 50000): 0, 2520, 5040, ..., 47880.
SELECT 'and 9-way rows:', arraySort(groupArray(k)) FROM tab_heap
WHERE hasAllTokens(s, ['tka', 'tkb', 'tkc', 'tkd', 'tke', 'tkf', 'tkg', 'tkh', 'tki'])
SETTINGS text_index_density_threshold = 1.0,
         log_comment = '04258_heap_9way_rows';

-- 11-way AND stresses the heap deeper.
-- LCM(2..10, 11) = 27720, then LCM with 13 = 360360.
-- floor(50000 / 360360) + 1 = 1 -> only row 0.
SELECT 'and 11-way:', count() FROM tab_heap
WHERE hasAllTokens(s, ['tka', 'tkb', 'tkc', 'tkd', 'tke', 'tkf', 'tkg', 'tkh', 'tki', 'tkj', 'tkk'])
SETTINGS text_index_density_threshold = 1.0,
         log_comment = '04258_heap_11way';

SELECT 'and 11-way rows:', arraySort(groupArray(k)) FROM tab_heap
WHERE hasAllTokens(s, ['tka', 'tkb', 'tkc', 'tkd', 'tke', 'tkf', 'tkg', 'tkh', 'tki', 'tkj', 'tkk'])
SETTINGS text_index_density_threshold = 1.0;

-- Equivalence: the materialize path must produce the same count for both shapes.
-- Without this, a heap-only bug producing the same wrong answer on both lazy
-- queries above would slip through.
SELECT 'materialize 9-way:', count() FROM tab_heap
WHERE hasAllTokens(s, ['tka', 'tkb', 'tkc', 'tkd', 'tke', 'tkf', 'tkg', 'tkh', 'tki'])
SETTINGS text_index_posting_list_apply_mode = 'materialize';

SELECT 'materialize 11-way:', count() FROM tab_heap
WHERE hasAllTokens(s, ['tka', 'tkb', 'tkc', 'tkd', 'tke', 'tkf', 'tkg', 'tkh', 'tki', 'tkj', 'tkk'])
SETTINGS text_index_posting_list_apply_mode = 'materialize';

-- Telemetry assertions: the heap variant must have run (LeapfrogIntersections > 0),
-- the brute-force variant must NOT have (the threshold forces leapfrog), and
-- advance() must have been called many times (every match in the heap calls
-- advance ~n times across iterations).
SYSTEM FLUSH LOGS query_log;

SELECT 'heap leapfrog telemetry:',
    sum(ProfileEvents['TextIndexLazyLeapfrogIntersections'])   > 0 AS leapfrog_fired,
    sum(ProfileEvents['TextIndexLazyBruteForceIntersections']) = 0 AS brute_force_not_fired,
    sum(ProfileEvents['TextIndexLazyAdvanceCount'])            > 9 AS advance_called_many_times
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment IN ('04258_heap_9way', '04258_heap_9way_rows', '04258_heap_11way');

DROP TABLE tab_heap;
