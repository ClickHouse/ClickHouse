-- Tests for segment advancement in lazy apply mode.
-- These tests specifically target the next()/seek() cross-segment behavior
-- that was fixed: next() must auto-advance to the next segment when the
-- current one is exhausted, and seek() must skip already-failed segments.
--
-- Key scenarios:
-- 1. next() crosses segment boundary (sequential scan via linearOr)
-- 2. next() crosses segment boundary in leapfrog intersection
-- 3. seek() crosses segment boundary in leapfrog intersection
-- 4. Mixed next()+seek() alternation across segment boundaries
-- 5. Single packed block per segment (block_size=128, exactly 129 docs per segment)
-- 6. Multiple segments with tail-only blocks (block_size=128, 129+1=130 docs)
-- 7. 3 segments with leapfrog 2-way intersection spanning all 3
-- 8. 4-way intersection across multiple segments
-- 9. Sparse token intersected with dense multi-segment token (leapfrog seek pattern)
-- 10. next() exhausts all segments correctly (is_valid=false)
-- 11. hasAnyTokens (OR) across multiple segments
-- 12. linearAnd (brute-force AND) across multiple segments
-- 13. Exact boundary: each segment has exactly 1 packed block (128 docs)
-- 14. Interleaved tokens in alternating segments

SET enable_full_text_index = 1;
SET allow_experimental_text_index_lazy_apply = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

----------------------------------------------------
SELECT 'Test 1: next() sequential scan across 3 segments via linearOr';

-- posting_list_block_size=128 means each segment holds 128 docs (+ first_doc_id for segment 0).
-- Token 'alpha' in all 500 rows -> 500 docs -> ~4 segments.
-- linearOr must traverse all segments to collect all doc IDs.

DROP TABLE IF EXISTS tab_next_linear;

CREATE TABLE tab_next_linear(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_next_linear SELECT number, 'alpha' FROM numbers(500);

SELECT count() FROM tab_next_linear WHERE hasToken(s, 'alpha');

-- Verify first and last doc_id are present
SELECT min(k), max(k) FROM tab_next_linear WHERE hasToken(s, 'alpha');

----------------------------------------------------
SELECT 'Test 2: next() in 2-way leapfrog across segments';

-- Two tokens both spanning multiple segments, with identical posting lists.
-- intersectTwo: c0.value()==c1.value() -> both call next() repeatedly.
-- next() must cross segment boundaries for both cursors.
-- Token 'aa' and 'bb' both appear in every row -> all 400 rows match AND.

DROP TABLE IF EXISTS tab_next_leapfrog;

CREATE TABLE tab_next_leapfrog(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_next_leapfrog SELECT number, 'aa bb' FROM numbers(400);

-- Both tokens in every row -> AND returns all 400
SELECT count() FROM tab_next_leapfrog WHERE hasAllTokens(s, ['aa', 'bb']);

-- Verify boundaries
SELECT min(k), max(k) FROM tab_next_leapfrog WHERE hasAllTokens(s, ['aa', 'bb']);

----------------------------------------------------
SELECT 'Test 3: seek() across segment boundary in leapfrog';

-- Token 'common' appears in every row (dense, multi-segment).
-- Token 'sparse' appears only every 200th row (sparse, few docs).
-- Leapfrog: sparse cursor leads, common cursor seeks to sparse values.
-- seek() on 'common' cursor must jump across segment boundaries.

DROP TABLE IF EXISTS tab_seek_cross_seg;

CREATE TABLE tab_seek_cross_seg(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_seek_cross_seg SELECT number,
    concat('common', if(number % 200 = 0, ' sparse', ''))
    FROM numbers(1000);

-- sparse appears at rows 0, 200, 400, 600, 800 -> 5 rows match AND
SELECT count() FROM tab_seek_cross_seg WHERE hasAllTokens(s, ['common', 'sparse']);

-- Verify exact rows
SELECT k FROM tab_seek_cross_seg WHERE hasAllTokens(s, ['common', 'sparse']) ORDER BY k;

----------------------------------------------------
SELECT 'Test 4: Mixed next+seek alternation across segment boundaries';

-- Token 'every' in all rows, token 'tenth' every 10th row.
-- Leapfrog: 'tenth' leads (sparse), 'every' follows with seek.
-- After match at 10k, both call next(); 'every' advances 1 by 1 while 'tenth' jumps 10.
-- This tests rapid alternation of next()/seek() across segment boundaries.

DROP TABLE IF EXISTS tab_mixed;

CREATE TABLE tab_mixed(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_mixed SELECT number,
    concat('every', if(number % 10 = 0, ' tenth', ''))
    FROM numbers(800);

-- AND: every row that has both -> every 10th row -> 80 rows
SELECT count() FROM tab_mixed WHERE hasAllTokens(s, ['every', 'tenth']);

SELECT min(k), max(k) FROM tab_mixed WHERE hasAllTokens(s, ['every', 'tenth']);

----------------------------------------------------
SELECT 'Test 5: Exactly 1 packed block per segment (129 docs per segment)';

-- posting_list_block_size=128 -> each segment holds 128 delta-encoded docs.
-- Segment 0: first_doc_id + 128 docs = 129 total.
-- With 258 rows all having the token: segment 0 has 129, segment 1 has 129.
-- Tests: segment boundary at row 129.

DROP TABLE IF EXISTS tab_single_pb;

CREATE TABLE tab_single_pb(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_single_pb SELECT number, 'uniform' FROM numbers(258);

SELECT count() FROM tab_single_pb WHERE hasToken(s, 'uniform');

-- Verify values around the segment boundary
-- Row 128 is in segment 0, row 129 should be in segment 1
SELECT k FROM tab_single_pb WHERE hasToken(s, 'uniform') AND k >= 127 AND k <= 130 ORDER BY k;

----------------------------------------------------
SELECT 'Test 6: 3 segments with 2-way leapfrog spanning all';

-- 'both' appears in every 3rd row across 1200 rows -> 400 docs -> ~3 segments.
-- 'partner' appears in every 3rd row, offset by 0 -> same rows as 'both'.
-- Leapfrog intersection must traverse all 3 segments for both cursors.

DROP TABLE IF EXISTS tab_three_seg;

CREATE TABLE tab_three_seg(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_three_seg SELECT number,
    if(number % 3 = 0, 'both partner', 'filler')
    FROM numbers(1200);

-- both AND partner: same set of rows -> 400 match
SELECT count() FROM tab_three_seg WHERE hasAllTokens(s, ['both', 'partner']);

-- Verify boundaries across segments
SELECT min(k), max(k) FROM tab_three_seg WHERE hasAllTokens(s, ['both', 'partner']);

----------------------------------------------------
SELECT 'Test 7: 4-way intersection across multiple segments';

-- All 4 tokens appear in every 5th row across 2000 rows -> 400 docs -> ~3 segments.
-- 4-way intersectFour must handle next() crossing segment boundaries for all 4 cursors.

DROP TABLE IF EXISTS tab_fourway_seg;

CREATE TABLE tab_fourway_seg(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_fourway_seg SELECT number,
    if(number % 5 = 0, 'wa wb wc wd', concat('x', toString(number % 5)))
    FROM numbers(2000);

-- 4-way AND: all 4 tokens present only in rows where k%5=0 -> 400 rows
SELECT count() FROM tab_fourway_seg WHERE hasAllTokens(s, ['wa', 'wb', 'wc', 'wd']);

SELECT min(k), max(k) FROM tab_fourway_seg WHERE hasAllTokens(s, ['wa', 'wb', 'wc', 'wd']);

----------------------------------------------------
SELECT 'Test 8: Sparse token seek across many segments of dense token';

-- 'dense' in all 3000 rows -> ~24 segments of 128.
-- 'rare' only at rows 0, 500, 1000, 1500, 2000, 2500 -> 6 rows.
-- Leapfrog: rare leads, dense seeks. seek() must jump across many segments.

DROP TABLE IF EXISTS tab_sparse_dense;

CREATE TABLE tab_sparse_dense(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_sparse_dense SELECT number,
    concat('dense', if(number % 500 = 0, ' rare', ''))
    FROM numbers(3000);

SELECT count() FROM tab_sparse_dense WHERE hasAllTokens(s, ['dense', 'rare']);

SELECT k FROM tab_sparse_dense WHERE hasAllTokens(s, ['dense', 'rare']) ORDER BY k;

----------------------------------------------------
SELECT 'Test 9: next() exhausts all segments (cursor becomes invalid)';

-- Verify that after all data is consumed, cursor correctly reports invalid.
-- 'only' appears in 300 rows -> ~3 segments.
-- Query must return exactly 300, meaning cursor didn't terminate early.

DROP TABLE IF EXISTS tab_exhaust;

CREATE TABLE tab_exhaust(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_exhaust SELECT number, 'only' FROM numbers(300);

SELECT count() FROM tab_exhaust WHERE hasToken(s, 'only');

-- Verify every single row is found by checking sum
SELECT sum(k) FROM tab_exhaust WHERE hasToken(s, 'only');

----------------------------------------------------
SELECT 'Test 10: hasAnyTokens (OR) across multiple segments';

-- 'left' in first 200 rows, 'right' in rows 200-399. No overlap.
-- Both span ~2 segments each.
-- OR should return all 400 rows.

DROP TABLE IF EXISTS tab_or_multi_seg;

CREATE TABLE tab_or_multi_seg(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_or_multi_seg SELECT number,
    if(number < 200, 'left', 'right')
    FROM numbers(400);

SELECT count() FROM tab_or_multi_seg WHERE hasAnyTokens(s, ['left', 'right']);

-- Each individually
SELECT count() FROM tab_or_multi_seg WHERE hasToken(s, 'left');

SELECT count() FROM tab_or_multi_seg WHERE hasToken(s, 'right');

----------------------------------------------------
SELECT 'Test 11: Brute-force AND (linearAnd) across multiple segments';

-- Force brute-force path by having very high density.
-- 'da' in all rows, 'db' in even rows. All span multiple segments.
-- linearOr + linearAnd must traverse all segments for both tokens.

DROP TABLE IF EXISTS tab_bf_multi_seg;

CREATE TABLE tab_bf_multi_seg(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_bf_multi_seg SELECT number,
    concat('da', if(number % 2 = 0, ' db', ''))
    FROM numbers(600);

-- brute-force AND: da(600) AND db(300) -> rows where k%2=0 -> 300
SELECT count() FROM tab_bf_multi_seg WHERE hasAllTokens(s, ['da', 'db']);

----------------------------------------------------
SELECT 'Test 12: Exactly at segment boundary - tail_size=0';

-- 128 docs per segment + first_doc_id = 129 rows in segment 0.
-- If we have exactly 129 rows, segment 0 has 1 full packed block, 0 tail.
-- No segment 1 needed. Count must be 129.
-- Then with 129*2=258 rows: segment 0=129, segment 1=129 (both perfectly aligned).

DROP TABLE IF EXISTS tab_tail_zero;

CREATE TABLE tab_tail_zero(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_tail_zero SELECT number, 'aligned' FROM numbers(129);

SELECT count() FROM tab_tail_zero WHERE hasToken(s, 'aligned');

DROP TABLE IF EXISTS tab_tail_zero2;

CREATE TABLE tab_tail_zero2(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_tail_zero2 SELECT number, 'aligned' FROM numbers(258);

SELECT count() FROM tab_tail_zero2 WHERE hasToken(s, 'aligned');

----------------------------------------------------
SELECT 'Test 13: Segment advance after merge with multiple parts';

-- Insert 3 separate parts, each with the same token spanning multiple segments.
-- After OPTIMIZE FINAL, the merged posting list will be very large.
-- Tests that merged multi-segment posting lists work correctly.

DROP TABLE IF EXISTS tab_merge_advance;

CREATE TABLE tab_merge_advance(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_merge_advance SELECT number, concat('merged', if(number % 3 = 0, ' extra', '')) FROM numbers(500);
INSERT INTO tab_merge_advance SELECT number + 500, concat('merged', if((number + 500) % 3 = 0, ' extra', '')) FROM numbers(500);
INSERT INTO tab_merge_advance SELECT number + 1000, concat('merged', if((number + 1000) % 3 = 0, ' extra', '')) FROM numbers(500);

OPTIMIZE TABLE tab_merge_advance FINAL;

-- 'merged' in all 1500 rows
SELECT count() FROM tab_merge_advance WHERE hasToken(s, 'merged');

-- AND: merged(1500) AND extra(500) -> rows where k%3=0 -> 500
SELECT count() FROM tab_merge_advance WHERE hasAllTokens(s, ['merged', 'extra']);

-- Verify exact first and last match of AND
SELECT min(k), max(k) FROM tab_merge_advance WHERE hasAllTokens(s, ['merged', 'extra']);

----------------------------------------------------
SELECT 'Test 14: 5-way leapfrog across multiple segments';

-- 5 tokens, each in every row. 5-way intersection uses intersectLeapfrogLinear (n=5).
-- All 5 cursors must advance across segment boundaries via next().

DROP TABLE IF EXISTS tab_fiveway_seg;

CREATE TABLE tab_fiveway_seg(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_fiveway_seg SELECT number, 'fa fb fc fd fe' FROM numbers(600);

-- All 5 in every row -> AND = 600
SELECT count() FROM tab_fiveway_seg WHERE hasAllTokens(s, ['fa', 'fb', 'fc', 'fd', 'fe']);

SELECT min(k), max(k) FROM tab_fiveway_seg WHERE hasAllTokens(s, ['fa', 'fb', 'fc', 'fd', 'fe']);

----------------------------------------------------
SELECT 'Test 15: Seek to exact segment boundary doc_id';

-- Token in every row, seek to the exact first doc of segment 1 (row 129).
-- This tests that seek() lands precisely at the segment boundary.

DROP TABLE IF EXISTS tab_seek_boundary;

CREATE TABLE tab_seek_boundary(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

-- 'full' in all 400 rows, 'pin' only at row 129 (exact segment 1 start)
INSERT INTO tab_seek_boundary SELECT number,
    concat('full', if(number = 129, ' pin', ''))
    FROM numbers(400);

-- AND: full(400) AND pin(1) -> only row 129
SELECT k FROM tab_seek_boundary WHERE hasAllTokens(s, ['full', 'pin']);

----------------------------------------------------
SELECT 'Test 16: Seek to last doc of a segment';

-- 'fill' in all 400 rows, 'last' only at row 128 (last doc of segment 0).
-- Segment 0: rows 0-128 (129 docs), so row 128 is the last in segment 0.

DROP TABLE IF EXISTS tab_seek_last;

CREATE TABLE tab_seek_last(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_seek_last SELECT number,
    concat('fill', if(number = 128, ' last', ''))
    FROM numbers(400);

SELECT k FROM tab_seek_last WHERE hasAllTokens(s, ['fill', 'last']);

----------------------------------------------------
SELECT 'Test 17: Interleaved sparse tokens across different segments';

-- 'tokena' at rows 0,3,6,... (every 3rd row)
-- 'tokenb' at rows 0,5,10,... (every 5th row)
-- AND -> rows where k%15=0: 0,15,30,...
-- Both tokens span multiple segments. Leapfrog must seek across segment boundaries.

DROP TABLE IF EXISTS tab_interleave;

CREATE TABLE tab_interleave(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_interleave SELECT number,
    concat(
        if(number % 3 = 0, 'tokena ', ''),
        if(number % 5 = 0, 'tokenb ', ''),
        'base'
    )
    FROM numbers(2000);

-- AND: tokena(667) AND tokenb(400) -> lcm(3,5)=15 -> rows where k%15=0 -> 134 rows (0,15,...,1995)
SELECT count() FROM tab_interleave WHERE hasAllTokens(s, ['tokena', 'tokenb']);

-- Verify first and last
SELECT min(k), max(k) FROM tab_interleave WHERE hasAllTokens(s, ['tokena', 'tokenb']);

----------------------------------------------------
SELECT 'Test 18: Very small posting list (embedded) mixed with large multi-segment posting list';

-- 'huge' in all 1000 rows (multi-segment), 'tiny' in only 3 rows (embedded posting).
-- The embedded cursor doesn't use segments at all.
-- Leapfrog must handle one embedded cursor + one multi-segment cursor correctly.

DROP TABLE IF EXISTS tab_embed_mix;

CREATE TABLE tab_embed_mix(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_embed_mix SELECT number,
    concat('huge', if(number IN (100, 500, 900), ' tiny', ''))
    FROM numbers(1000);

-- AND: huge(1000) AND tiny(3) -> rows 100, 500, 900
SELECT count() FROM tab_embed_mix WHERE hasAllTokens(s, ['huge', 'tiny']);

SELECT k FROM tab_embed_mix WHERE hasAllTokens(s, ['huge', 'tiny']) ORDER BY k;

----------------------------------------------------
SELECT 'Test 19: next() across many segments - stress test';

-- 'stress' in all 5000 rows -> ~39 segments of 128.
-- Full sequential scan via hasToken must read all of them.

DROP TABLE IF EXISTS tab_stress;

CREATE TABLE tab_stress(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_stress SELECT number, 'stress' FROM numbers(5000);

SELECT count() FROM tab_stress WHERE hasToken(s, 'stress');

-- Verify sum to ensure no data loss
SELECT sum(k) FROM tab_stress WHERE hasToken(s, 'stress');

----------------------------------------------------
SELECT 'Test 20: 2-way intersection - one cursor finishes early across segments';

-- 'short' in first 150 rows (just over 1 segment).
-- 'long' in all 500 rows (4 segments).
-- AND: intersection stops when 'short' cursor becomes invalid after segment 1.
-- Must still find all 150 matches correctly.

DROP TABLE IF EXISTS tab_early_finish;

CREATE TABLE tab_early_finish(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_early_finish SELECT number,
    concat('long', if(number < 150, ' short', ''))
    FROM numbers(500);

-- AND: long(500) AND short(150) -> first 150 rows
SELECT count() FROM tab_early_finish WHERE hasAllTokens(s, ['long', 'short']);

SELECT min(k), max(k) FROM tab_early_finish WHERE hasAllTokens(s, ['long', 'short']);

----------------------------------------------------
-- Cleanup
DROP TABLE IF EXISTS tab_next_linear;
DROP TABLE IF EXISTS tab_next_leapfrog;
DROP TABLE IF EXISTS tab_seek_cross_seg;
DROP TABLE IF EXISTS tab_mixed;
DROP TABLE IF EXISTS tab_single_pb;
DROP TABLE IF EXISTS tab_three_seg;
DROP TABLE IF EXISTS tab_fourway_seg;
DROP TABLE IF EXISTS tab_sparse_dense;
DROP TABLE IF EXISTS tab_exhaust;
DROP TABLE IF EXISTS tab_or_multi_seg;
DROP TABLE IF EXISTS tab_bf_multi_seg;
DROP TABLE IF EXISTS tab_tail_zero;
DROP TABLE IF EXISTS tab_tail_zero2;
DROP TABLE IF EXISTS tab_merge_advance;
DROP TABLE IF EXISTS tab_fiveway_seg;
DROP TABLE IF EXISTS tab_seek_boundary;
DROP TABLE IF EXISTS tab_seek_last;
DROP TABLE IF EXISTS tab_interleave;
DROP TABLE IF EXISTS tab_embed_mix;
DROP TABLE IF EXISTS tab_stress;
DROP TABLE IF EXISTS tab_early_finish;
