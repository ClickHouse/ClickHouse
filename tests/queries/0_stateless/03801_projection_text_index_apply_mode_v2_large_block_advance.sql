-- Tags: no-fasttest
-- no-fasttest: It can be slow

-- Tests for large block advancement in V2 lazy apply mode.
-- These tests specifically target the next()/seek() cross-large-block behavior
-- that was fixed: next() must auto-advance to the next large block when the
-- current one is exhausted, and seek() must skip already-failed large blocks.
--
-- Key scenarios:
-- 1. next() crosses large block boundary (sequential scan via linearOr)
-- 2. next() crosses large block boundary in leapfrog intersection
-- 3. seek() crosses large block boundary in leapfrog intersection
-- 4. Mixed next()+seek() alternation across large block boundaries
-- 5. Single packed block per large block (block_size=128, exactly 129 docs per LB)
-- 6. Multiple large blocks with tail-only blocks (block_size=128, 129+1=130 docs)
-- 7. 3 large blocks with leapfrog 2-way intersection spanning all 3
-- 8. 4-way intersection across multiple large blocks
-- 9. Sparse token intersected with dense multi-LB token (leapfrog seek pattern)
-- 10. next() exhausts all large blocks correctly (is_valid=false)
-- 11. hasAnyTokens (OR) across multiple large blocks
-- 12. linearAnd (brute-force AND) across multiple large blocks
-- 13. Exact boundary: each large block has exactly 1 packed block (128 docs)
-- 14. Interleaved tokens in alternating large blocks

SET enable_full_text_index = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;
SET use_skip_indexes_on_data_read = 0;

----------------------------------------------------
SELECT 'Test 1: next() sequential scan across 3 large blocks via linearOr';

-- posting_list_block_size=128 means each large block holds 128 docs (+ first_doc_id for LB0).
-- Token 'alpha' in all 500 rows -> 500 docs -> ~4 large blocks.
-- linearOr must traverse all large blocks to collect all doc IDs.

DROP TABLE IF EXISTS tab_next_linear;

CREATE TABLE tab_next_linear(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_next_linear SELECT number, 'alpha' FROM numbers(500);

SELECT count() FROM tab_next_linear WHERE hasToken(s, 'alpha');

-- Verify first and last doc_id are present
SELECT min(k), max(k) FROM tab_next_linear WHERE hasToken(s, 'alpha');

----------------------------------------------------
SELECT 'Test 2: next() in 2-way leapfrog across large blocks';

-- Two tokens both spanning multiple large blocks, with identical posting lists.
-- intersectTwo: c0.value()==c1.value() -> both call next() repeatedly.
-- next() must cross large block boundaries for both cursors.
-- Token 'aa' and 'bb' both appear in every row -> all 400 rows match AND.

DROP TABLE IF EXISTS tab_next_leapfrog;

CREATE TABLE tab_next_leapfrog(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_next_leapfrog SELECT number, 'aa bb' FROM numbers(400);

-- Both tokens in every row -> AND returns all 400
SELECT count() FROM tab_next_leapfrog WHERE hasAllTokens(s, ['aa', 'bb']);

-- Verify boundaries
SELECT min(k), max(k) FROM tab_next_leapfrog WHERE hasAllTokens(s, ['aa', 'bb']);

----------------------------------------------------
SELECT 'Test 3: seek() across large block boundary in leapfrog';

-- Token 'common' appears in every row (dense, multi-LB).
-- Token 'sparse' appears only every 200th row (sparse, few docs).
-- Leapfrog: sparse cursor leads, common cursor seeks to sparse values.
-- seek() on 'common' cursor must jump across large block boundaries.

DROP TABLE IF EXISTS tab_seek_cross_lb;

CREATE TABLE tab_seek_cross_lb(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_seek_cross_lb SELECT number,
    concat('common', if(number % 200 = 0, ' sparse', ''))
    FROM numbers(1000);

-- sparse appears at rows 0, 200, 400, 600, 800 -> 5 rows match AND
SELECT count() FROM tab_seek_cross_lb WHERE hasAllTokens(s, ['common', 'sparse']);

-- Verify exact rows
SELECT k FROM tab_seek_cross_lb WHERE hasAllTokens(s, ['common', 'sparse']) ORDER BY k;

----------------------------------------------------
SELECT 'Test 4: Mixed next+seek alternation across large block boundaries';

-- Token 'every' in all rows, token 'tenth' every 10th row.
-- Leapfrog: 'tenth' leads (sparse), 'every' follows with seek.
-- After match at 10k, both call next(); 'every' advances 1 by 1 while 'tenth' jumps 10.
-- This tests rapid alternation of next()/seek() across LB boundaries.

DROP TABLE IF EXISTS tab_mixed;

CREATE TABLE tab_mixed(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_mixed SELECT number,
    concat('every', if(number % 10 = 0, ' tenth', ''))
    FROM numbers(800);

-- AND: every row that has both -> every 10th row -> 80 rows
SELECT count() FROM tab_mixed WHERE hasAllTokens(s, ['every', 'tenth']);

SELECT min(k), max(k) FROM tab_mixed WHERE hasAllTokens(s, ['every', 'tenth']);

----------------------------------------------------
SELECT 'Test 5: Exactly 1 packed block per large block (129 docs per LB)';

-- posting_list_block_size=128 -> each LB holds 128 delta-encoded docs.
-- LB0: first_doc_id + 128 docs = 129 total.
-- With 258 rows all having the token: LB0 has 129, LB1 has 129.
-- Tests: LB boundary at row 129.

DROP TABLE IF EXISTS tab_single_pb;

CREATE TABLE tab_single_pb(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_single_pb SELECT number, 'uniform' FROM numbers(258);

SELECT count() FROM tab_single_pb WHERE hasToken(s, 'uniform');

-- Verify values around the LB boundary
-- Row 128 is in LB0, row 129 should be in LB1
SELECT k FROM tab_single_pb WHERE hasToken(s, 'uniform') AND k >= 127 AND k <= 130 ORDER BY k;

----------------------------------------------------
SELECT 'Test 6: 3 large blocks with 2-way leapfrog spanning all';

-- 'both' appears in every 3rd row across 1200 rows -> 400 docs -> ~3 LBs.
-- 'partner' appears in every 3rd row, offset by 0 -> same rows as 'both'.
-- Leapfrog intersection must traverse all 3 LBs for both cursors.

DROP TABLE IF EXISTS tab_three_lb;

CREATE TABLE tab_three_lb(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_three_lb SELECT number,
    if(number % 3 = 0, 'both partner', 'filler')
    FROM numbers(1200);

-- both AND partner: same set of rows -> 400 match
SELECT count() FROM tab_three_lb WHERE hasAllTokens(s, ['both', 'partner']);

-- Verify boundaries across LBs
SELECT min(k), max(k) FROM tab_three_lb WHERE hasAllTokens(s, ['both', 'partner']);

----------------------------------------------------
SELECT 'Test 7: 4-way intersection across multiple large blocks';

-- All 4 tokens appear in every 5th row across 2000 rows -> 400 docs -> ~3 LBs.
-- 4-way intersectFour must handle next() crossing LB boundaries for all 4 cursors.

DROP TABLE IF EXISTS tab_fourway_lb;

CREATE TABLE tab_fourway_lb(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_fourway_lb SELECT number,
    if(number % 5 = 0, 'wa wb wc wd', concat('x', toString(number % 5)))
    FROM numbers(2000);

-- 4-way AND: all 4 tokens present only in rows where k%5=0 -> 400 rows
SELECT count() FROM tab_fourway_lb WHERE hasAllTokens(s, ['wa', 'wb', 'wc', 'wd']);

SELECT min(k), max(k) FROM tab_fourway_lb WHERE hasAllTokens(s, ['wa', 'wb', 'wc', 'wd']);

----------------------------------------------------
SELECT 'Test 8: Sparse token seek across many large blocks of dense token';

-- 'dense' in all 3000 rows -> ~24 large blocks of 128.
-- 'rare' only at rows 0, 500, 1000, 1500, 2000, 2500 -> 6 rows.
-- Leapfrog: rare leads, dense seeks. seek() must jump across many LBs.

DROP TABLE IF EXISTS tab_sparse_dense;

CREATE TABLE tab_sparse_dense(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_sparse_dense SELECT number,
    concat('dense', if(number % 500 = 0, ' rare', ''))
    FROM numbers(3000);

SELECT count() FROM tab_sparse_dense WHERE hasAllTokens(s, ['dense', 'rare']);

SELECT k FROM tab_sparse_dense WHERE hasAllTokens(s, ['dense', 'rare']) ORDER BY k;

----------------------------------------------------
SELECT 'Test 9: next() exhausts all large blocks (cursor becomes invalid)';

-- Verify that after all data is consumed, cursor correctly reports invalid.
-- 'only' appears in 300 rows -> ~3 LBs.
-- Query must return exactly 300, meaning cursor didn't terminate early.

DROP TABLE IF EXISTS tab_exhaust;

CREATE TABLE tab_exhaust(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_exhaust SELECT number, 'only' FROM numbers(300);

SELECT count() FROM tab_exhaust WHERE hasToken(s, 'only');

-- Verify every single row is found by checking sum
SELECT sum(k) FROM tab_exhaust WHERE hasToken(s, 'only');

----------------------------------------------------
SELECT 'Test 10: hasAnyTokens (OR) across multiple large blocks';

-- 'left' in first 200 rows, 'right' in rows 200-399. No overlap.
-- Both span ~2 LBs each.
-- OR should return all 400 rows.

DROP TABLE IF EXISTS tab_or_multi_lb;

CREATE TABLE tab_or_multi_lb(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_or_multi_lb SELECT number,
    if(number < 200, 'left', 'right')
    FROM numbers(400);

SELECT count() FROM tab_or_multi_lb WHERE hasAnyTokens(s, ['left', 'right']);

-- Each individually
SELECT count() FROM tab_or_multi_lb WHERE hasToken(s, 'left');

SELECT count() FROM tab_or_multi_lb WHERE hasToken(s, 'right');

----------------------------------------------------
SELECT 'Test 11: Brute-force AND (linearAnd) across multiple large blocks';

-- Force brute-force path by having very high density.
-- 'da' in all rows, 'db' in even rows. All span multiple LBs.
-- linearOr + linearAnd must traverse all LBs for both tokens.

DROP TABLE IF EXISTS tab_bf_multi_lb;

CREATE TABLE tab_bf_multi_lb(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_bf_multi_lb SELECT number,
    concat('da', if(number % 2 = 0, ' db', ''))
    FROM numbers(600);

-- brute-force AND: da(600) AND db(300) -> rows where k%2=0 -> 300
SELECT count() FROM tab_bf_multi_lb WHERE hasAllTokens(s, ['da', 'db']);

----------------------------------------------------
SELECT 'Test 12: Exactly at large block boundary - tail_size=0';

-- 128 docs per LB + first_doc_id = 129 rows in LB0.
-- If we have exactly 129 rows, LB0 has 1 full packed block, 0 tail.
-- No LB1 needed. Count must be 129.
-- Then with 129*2=258 rows: LB0=129, LB1=129 (both perfectly aligned).

DROP TABLE IF EXISTS tab_tail_zero;

CREATE TABLE tab_tail_zero(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_tail_zero SELECT number, 'aligned' FROM numbers(129);

SELECT count() FROM tab_tail_zero WHERE hasToken(s, 'aligned');

DROP TABLE IF EXISTS tab_tail_zero2;

CREATE TABLE tab_tail_zero2(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_tail_zero2 SELECT number, 'aligned' FROM numbers(258);

SELECT count() FROM tab_tail_zero2 WHERE hasToken(s, 'aligned');

----------------------------------------------------
SELECT 'Test 13: Large block advance after merge with multiple parts';

-- Insert 3 separate parts, each with the same token spanning multiple LBs.
-- After OPTIMIZE FINAL, the merged posting list will be very large.
-- Tests that merged multi-LB posting lists work correctly.

DROP TABLE IF EXISTS tab_merge_advance;

CREATE TABLE tab_merge_advance(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
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
SELECT 'Test 14: 5-way leapfrog across multiple large blocks';

-- 5 tokens, each in every row. 5-way intersection uses intersectLeapfrogLinear (n=5).
-- All 5 cursors must advance across LB boundaries via next().

DROP TABLE IF EXISTS tab_fiveway_lb;

CREATE TABLE tab_fiveway_lb(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_fiveway_lb SELECT number, 'fa fb fc fd fe' FROM numbers(600);

-- All 5 in every row -> AND = 600
SELECT count() FROM tab_fiveway_lb WHERE hasAllTokens(s, ['fa', 'fb', 'fc', 'fd', 'fe']);

SELECT min(k), max(k) FROM tab_fiveway_lb WHERE hasAllTokens(s, ['fa', 'fb', 'fc', 'fd', 'fe']);

----------------------------------------------------
SELECT 'Test 15: Seek to exact large block boundary doc_id';

-- Token in every row, seek to the exact first doc of LB1 (row 129).
-- This tests that seek() lands precisely at the LB boundary.

DROP TABLE IF EXISTS tab_seek_boundary;

CREATE TABLE tab_seek_boundary(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

-- 'full' in all 400 rows, 'pin' only at row 129 (exact LB1 start)
INSERT INTO tab_seek_boundary SELECT number,
    concat('full', if(number = 129, ' pin', ''))
    FROM numbers(400);

-- AND: full(400) AND pin(1) -> only row 129
SELECT k FROM tab_seek_boundary WHERE hasAllTokens(s, ['full', 'pin']);

----------------------------------------------------
SELECT 'Test 16: Seek to last doc of a large block';

-- 'fill' in all 400 rows, 'last' only at row 128 (last doc of LB0).
-- LB0: rows 0-128 (129 docs), so row 128 is the last in LB0.

DROP TABLE IF EXISTS tab_seek_last;

CREATE TABLE tab_seek_last(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_seek_last SELECT number,
    concat('fill', if(number = 128, ' last', ''))
    FROM numbers(400);

SELECT k FROM tab_seek_last WHERE hasAllTokens(s, ['fill', 'last']);

----------------------------------------------------
SELECT 'Test 17: Interleaved sparse tokens across different large blocks';

-- 'tokena' at rows 0,3,6,... (every 3rd row)
-- 'tokenb' at rows 0,5,10,... (every 5th row)
-- AND -> rows where k%15=0: 0,15,30,...
-- Both tokens span multiple LBs. Leapfrog must seek across LB boundaries.

DROP TABLE IF EXISTS tab_interleave;

CREATE TABLE tab_interleave(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
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
SELECT 'Test 18: Very small posting list (embedded) mixed with large multi-LB posting list';

-- 'huge' in all 1000 rows (multi-LB), 'tiny' in only 3 rows (embedded posting).
-- The embedded cursor doesn't use large blocks at all.
-- Leapfrog must handle one embedded cursor + one multi-LB cursor correctly.

DROP TABLE IF EXISTS tab_embed_mix;

CREATE TABLE tab_embed_mix(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_embed_mix SELECT number,
    concat('huge', if(number IN (100, 500, 900), ' tiny', ''))
    FROM numbers(1000);

-- AND: huge(1000) AND tiny(3) -> rows 100, 500, 900
SELECT count() FROM tab_embed_mix WHERE hasAllTokens(s, ['huge', 'tiny']);

SELECT k FROM tab_embed_mix WHERE hasAllTokens(s, ['huge', 'tiny']) ORDER BY k;

----------------------------------------------------
SELECT 'Test 19: next() across many large blocks - stress test';

-- 'stress' in all 5000 rows -> ~39 large blocks of 128.
-- Full sequential scan via hasToken must read all of them.

DROP TABLE IF EXISTS tab_stress;

CREATE TABLE tab_stress(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_stress SELECT number, 'stress' FROM numbers(5000);

SELECT count() FROM tab_stress WHERE hasToken(s, 'stress');

-- Verify sum to ensure no data loss
SELECT sum(k) FROM tab_stress WHERE hasToken(s, 'stress');

----------------------------------------------------
SELECT 'Test 20: 2-way intersection - one cursor finishes early across LBs';

-- 'short' in first 150 rows (just over 1 LB).
-- 'long' in all 500 rows (4 LBs).
-- AND: intersection stops when 'short' cursor becomes invalid after LB1.
-- Must still find all 150 matches correctly.

DROP TABLE IF EXISTS tab_early_finish;

CREATE TABLE tab_early_finish(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
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
DROP TABLE IF EXISTS tab_seek_cross_lb;
DROP TABLE IF EXISTS tab_mixed;
DROP TABLE IF EXISTS tab_single_pb;
DROP TABLE IF EXISTS tab_three_lb;
DROP TABLE IF EXISTS tab_fourway_lb;
DROP TABLE IF EXISTS tab_sparse_dense;
DROP TABLE IF EXISTS tab_exhaust;
DROP TABLE IF EXISTS tab_or_multi_lb;
DROP TABLE IF EXISTS tab_bf_multi_lb;
DROP TABLE IF EXISTS tab_tail_zero;
DROP TABLE IF EXISTS tab_tail_zero2;
DROP TABLE IF EXISTS tab_merge_advance;
DROP TABLE IF EXISTS tab_fiveway_lb;
DROP TABLE IF EXISTS tab_seek_boundary;
DROP TABLE IF EXISTS tab_seek_last;
DROP TABLE IF EXISTS tab_interleave;
DROP TABLE IF EXISTS tab_embed_mix;
DROP TABLE IF EXISTS tab_stress;
DROP TABLE IF EXISTS tab_early_finish;
