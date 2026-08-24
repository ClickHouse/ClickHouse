-- Verifies that the nine TextIndexLazy* ProfileEvents fire when the corresponding
-- code paths in MergeTreeIndexTextPostingListCursor are exercised. Without this guard,
-- a regression that silently disables a skip optimisation (or never reaches the lazy
-- cursor path) would not be caught: the query results would stay correct because the
-- materialize path is functionally equivalent.
--
-- The skip counters are shared between the OR and AND paths: `SegmentsSkippedDense`
-- (dense segment padded whole), `SegmentsSkippedResolved` and `BlocksSkippedResolved`
-- (region already resolved: all-ones for OR, all-zeros for AND). The OR and AND queries
-- below still exercise both sides of each counter.

SET enable_full_text_index = 1;
SET allow_experimental_text_index_lazy_apply = 1;
SET text_index_posting_list_apply_mode = 'lazy';
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET use_query_condition_cache = 0;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS tab_lazy_pe;

-- posting_list_block_size = 256 -> each non-embedded segment holds up to 256 docs in
-- 2 packed blocks of 128. Tokens with cardinality <= 256 are stored with `SingleBlock`
-- flag and preloaded as rare-token postings (embedded path); tokens with cardinality
-- > 256 take the lazy cursor's multi-segment / multi-block path. The data below
-- mixes both so we exercise all eight profile events.
-- `index_granularity_bytes = 0` disables adaptive granularity so the 2000 rows stay in a
-- single granule regardless of the CI's randomized `index_granularity_bytes`. With multiple
-- granules, queries whose tokens occupy disjoint row ranges (Q6: csubset & eright) short-
-- circuit per-granule before `lazyIntersectPostingLists` runs, leaving the AND side of
-- `SegmentsSkippedResolved` unfired.
CREATE TABLE tab_lazy_pe(
    k UInt64,
    s String,
    INDEX idx s TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking',
        posting_list_block_size = 256))
ENGINE = MergeTree() ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = '10M';

-- Tokens are named so their alphabetical order ('a' < 'b' < 'c' < 'd' < 'e') matches
-- the desired position in `TextSearchQuery::tokens` after the constructor's sort —
-- the lazy cursor map iterates in sorted order, so `cursors[0]` becomes c0 (linearOr
-- in brute-force) and `cursors[1]` becomes c1 (linearAnd).
--
--   adense    : every row 0..1999            -> 2000 docs, 8 dense multi-segments
--   bnarrow   : rows 0..127                  -> 128 docs, SingleBlock => embedded path
--   csubset   : rows 0..299                  -> 300 docs, 2 dense multi-segments
--   dwide     : rows 0..127, 256..383,
--               512..639, 768..895           -> 512 docs, 2 multi-block segments
--   eright    : rows 1700..1999              -> 300 docs, 2 dense multi-segments
INSERT INTO tab_lazy_pe
SELECT number,
    concat(
        'adense',
        if(number < 128, ' bnarrow', ''),
        if(number < 300, ' csubset', ''),
        if((number < 128)
            OR (number >= 256 AND number <= 383)
            OR (number >= 512 AND number <= 639)
            OR (number >= 768 AND number <= 895), ' dwide', ''),
        if(number >= 1700, ' eright', '')
    )
FROM numbers(2000);

-- Sanity-check token cardinalities. A subtle off-by-one in the data generator would
-- silently invalidate the event assertions below.
SELECT 'cardinality adense',  count() FROM tab_lazy_pe WHERE hasToken(s, 'adense');
SELECT 'cardinality bnarrow', count() FROM tab_lazy_pe WHERE hasToken(s, 'bnarrow');
SELECT 'cardinality csubset', count() FROM tab_lazy_pe WHERE hasToken(s, 'csubset');
SELECT 'cardinality dwide',   count() FROM tab_lazy_pe WHERE hasToken(s, 'dwide');
SELECT 'cardinality eright',  count() FROM tab_lazy_pe WHERE hasToken(s, 'eright');

-- ===========================================================================
-- Tagged queries. Each query carries a unique log_comment so the subsequent
-- system.query_log aggregation can isolate its ProfileEvents row.
-- ===========================================================================

-- Q1: linearOr on a non-dense multi-segment token decodes its packed blocks.
--     Triggers: PackedBlocksDecoded, SegmentsPrepared.
SELECT count() FROM tab_lazy_pe WHERE hasToken(s, 'dwide')
    SETTINGS log_comment = '04257_pe_or_blocks_decoded';

-- Q2: linearOr on a fully-dense multi-segment token hits Level-1 dense memset for
--     every segment. Triggers: SegmentsSkippedDense.
SELECT count() FROM tab_lazy_pe WHERE hasToken(s, 'adense')
    SETTINGS log_comment = '04257_pe_or_seg_dense';

-- Q3: hasAnyTokens(['adense', 'csubset']) -> 'adense' (density 1.0) sorts before
--     'csubset' (density 1.0, stable sort by tokens kept it second). 'adense' fills
--     bits 0..1999; csubset's two segments [0..255] and [256..299] are then fully
--     covered. Triggers: SegmentsSkippedResolved (OR side).
SELECT count() FROM tab_lazy_pe WHERE hasAnyTokens(s, ['adense', 'csubset'])
    SETTINGS log_comment = '04257_pe_or_seg_covered';

-- Q4: hasAnyTokens(['bnarrow', 'dwide']) -> bnarrow (embedded, density 1.0) sorts
--     before dwide (density 0.57). bnarrow fills bits 0..127; dwide's segment
--     [0..383] is not fully covered, but its packed block 0 (doc range 0..127) IS.
--     Triggers: BlocksSkippedResolved (OR side).
SELECT count() FROM tab_lazy_pe WHERE hasAnyTokens(s, ['bnarrow', 'dwide'])
    SETTINGS log_comment = '04257_pe_or_block_covered';

-- Q5: brute-force AND ['adense', 'csubset'] (min_density = 1.0 >= threshold 0.0).
--     c0='adense'->linearOr fills via Level-1 dense; c1='csubset'->linearAnd hits
--     its two dense segments. Triggers: SegmentsSkippedDense (AND side), BruteForceIntersections.
SELECT count() FROM tab_lazy_pe WHERE hasAllTokens(s, ['adense', 'csubset'])
    SETTINGS text_index_density_threshold = 0.0,
             log_comment = '04257_pe_and_seg_dense';

-- Q6: brute-force AND ['csubset', 'eright'] - disjoint ranges.
--     c0='csubset' fills 0..299; c1='eright''s segments [1700..1955] and [1956..1999]
--     are all-zero in the output. Triggers: SegmentsSkippedResolved (AND side).
SELECT count() FROM tab_lazy_pe WHERE hasAllTokens(s, ['csubset', 'eright'])
    SETTINGS text_index_density_threshold = 0.0,
             log_comment = '04257_pe_and_seg_zero';

-- Q7: brute-force AND ['bnarrow', 'dwide'] - narrow c0 leaves block-level zeros in c1.
--     c0='bnarrow' fills bits 0..127; c1='dwide' segment [0..383]:
--       - segment not fully zero (block 0 has bits) so Level-2a doesn't fire,
--       - segment not dense so Level-1 doesn't fire,
--       - block 0 [0..127] not zero -> decode normally,
--       - block 1 [128..383] all-zero in output -> BlocksSkippedResolved (AND side) fires.
SELECT count() FROM tab_lazy_pe WHERE hasAllTokens(s, ['bnarrow', 'dwide'])
    SETTINGS text_index_density_threshold = 0.0,
             log_comment = '04257_pe_and_block_zero';

-- Q8: leapfrog AND ['adense', 'dwide'] forced by threshold 1.0 against min_density=0.57.
--     intersectLeapfrog dispatches to intersectTwo, which calls advance() repeatedly.
--     Triggers: LeapfrogIntersections, AdvanceCount.
SELECT count() FROM tab_lazy_pe WHERE hasAllTokens(s, ['adense', 'dwide'])
    SETTINGS text_index_density_threshold = 1.0,
             log_comment = '04257_pe_alg_leapfrog';

-- ===========================================================================
-- Assertions: every event must fire (>= once) across the eight tagged queries.
-- A single missing event is enough to fail the test.
-- ===========================================================================

SYSTEM FLUSH LOGS query_log;

SELECT 'all events fired across tagged queries:';
SELECT
    sum(ProfileEvents['TextIndexLazyPackedBlocksDecoded'])      > 0 AS blocks_decoded,
    sum(ProfileEvents['TextIndexLazySegmentsPrepared'])         > 0 AS segs_prepared,
    sum(ProfileEvents['TextIndexLazySegmentsBuilt'])            > 0 AS segs_built,
    sum(ProfileEvents['TextIndexLazySegmentsSkippedDense'])     > 0 AS seg_dense,
    sum(ProfileEvents['TextIndexLazySegmentsSkippedResolved'])  > 0 AS seg_resolved,
    sum(ProfileEvents['TextIndexLazyBlocksSkippedResolved'])    > 0 AS block_resolved,
    sum(ProfileEvents['TextIndexLazyBruteForceIntersections'])  > 0 AS brute_force,
    sum(ProfileEvents['TextIndexLazyLeapfrogIntersections'])    > 0 AS leapfrog,
    sum(ProfileEvents['TextIndexLazyAdvanceCount'])             > 0 AS advance_called
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment LIKE '04257_pe_%';

DROP TABLE tab_lazy_pe;
