-- Tags: no-fasttest
-- no-fasttest: It can be slow

-- Tests for the linearOr/linearAnd block-level skip optimization.
-- These verify that using packed_block_last_doc_ids[] to skip packed blocks
-- during linear scans produces correct results, and that middle blocks
-- (fully within the query range) skip binary search clipping without error.
--
-- Key scenarios:
-- 1. Dense token spanning many packed blocks, narrow query range (block skip)
-- 2. Brute-force AND with high-density tokens spanning many blocks
-- 3. linearOr across multiple large blocks with partial range overlap
-- 4. Query range exactly aligned to packed block boundaries
-- 5. Query range spanning only a single packed block
-- 6. Query range covering all packed blocks (no skip, all middle blocks)
-- 7. Brute-force AND where one token is narrow-range (partial block overlap)
-- 8. linearOr with embedded + large posting list mix

SET enable_full_text_index = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;
SET use_skip_indexes_on_data_read = 0;

----------------------------------------------------
SELECT 'Test 1: Dense token, narrow query range — block skip effectiveness';

-- 'freq' in all 5000 rows -> ~39 large blocks of 128 -> many packed blocks.
-- 'pin' only at row 2500. AND forces leapfrog/brute-force to process 'freq'
-- only in the packed block(s) near row 2500.
-- This tests that linearOr/linearAnd skip blocks far from the query range.

DROP TABLE IF EXISTS tab_skip1;

CREATE TABLE tab_skip1(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_skip1 SELECT number, concat('freq', if(number = 2500, ' pin', '')) FROM numbers(5000);

SELECT count() FROM tab_skip1 WHERE hasAllTokens(s, ['freq', 'pin']);
SELECT k FROM tab_skip1 WHERE hasAllTokens(s, ['freq', 'pin']);

----------------------------------------------------
SELECT 'Test 2: Brute-force AND with two dense multi-block tokens';

-- 'da' in all 2000 rows (many blocks), 'db' in even rows (1000 docs, also many blocks).
-- Both are high density -> brute-force path (linearOr + linearAnd).
-- Tests that linearAndImpl correctly skips blocks and clips first/last blocks.

DROP TABLE IF EXISTS tab_skip2;

CREATE TABLE tab_skip2(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_skip2 SELECT number, concat('da', if(number % 2 = 0, ' db', '')) FROM numbers(2000);

SELECT count() FROM tab_skip2 WHERE hasAllTokens(s, ['da', 'db']);

-- Verify first and last
SELECT min(k), max(k) FROM tab_skip2 WHERE hasAllTokens(s, ['da', 'db']);

----------------------------------------------------
SELECT 'Test 3: linearOr across multiple large blocks with partial overlap';

-- 'wide' in all 3000 rows -> ~24 large blocks. Query all rows via hasToken.
-- linearOr must traverse all large blocks, each invoking linearOrImpl which
-- uses the block-skip optimization. Full coverage: all blocks overlap.

DROP TABLE IF EXISTS tab_skip3;

CREATE TABLE tab_skip3(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_skip3 SELECT number, 'wide' FROM numbers(3000);

SELECT count() FROM tab_skip3 WHERE hasToken(s, 'wide');
SELECT sum(k) FROM tab_skip3 WHERE hasToken(s, 'wide');

----------------------------------------------------
SELECT 'Test 4: Query range aligned to packed block boundaries';

-- 'aligned' in all 512 rows -> 4 full packed blocks (128 each) in one large block.
-- (first_doc_id = 0, then 511 delta-encoded docs = 3 full blocks + 1 block of 127 + first_doc_id)
-- Actually: 512 rows = first_doc_id + 511 docs = 3 full blocks (384) + tail of 127 = 4 blocks.
-- Intersect with 'mark' at rows 0, 128, 256, 384 — exactly at block boundaries.

DROP TABLE IF EXISTS tab_skip4;

CREATE TABLE tab_skip4(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 4096))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_skip4 SELECT number, concat('aligned', if(number % 128 = 0, ' mark', '')) FROM numbers(512);

SELECT count() FROM tab_skip4 WHERE hasAllTokens(s, ['aligned', 'mark']);
SELECT k FROM tab_skip4 WHERE hasAllTokens(s, ['aligned', 'mark']) ORDER BY k;

----------------------------------------------------
SELECT 'Test 5: Query range spanning single packed block';

-- 'full' in all 1000 rows. 'narrow' only at rows 65 and 66 — within packed block 0
-- (which covers doc_ids 0..128 for large block 0).
-- linearOr/linearAnd for 'full' should decode only the block containing rows 65-66.

DROP TABLE IF EXISTS tab_skip5;

CREATE TABLE tab_skip5(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_skip5 SELECT number, concat('full', if(number IN (65, 66), ' narrow', '')) FROM numbers(1000);

SELECT count() FROM tab_skip5 WHERE hasAllTokens(s, ['full', 'narrow']);
SELECT k FROM tab_skip5 WHERE hasAllTokens(s, ['full', 'narrow']) ORDER BY k;

----------------------------------------------------
SELECT 'Test 6: Query range covering all packed blocks (no skip)';

-- 'every' in all 600 rows. Single token query -> linearOr with full range.
-- All packed blocks overlap the query range. Middle blocks should skip clipping.
-- Verify every row is found.

DROP TABLE IF EXISTS tab_skip6;

CREATE TABLE tab_skip6(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_skip6 SELECT number, 'every' FROM numbers(600);

SELECT count() FROM tab_skip6 WHERE hasToken(s, 'every');
SELECT min(k), max(k) FROM tab_skip6 WHERE hasToken(s, 'every');

----------------------------------------------------
SELECT 'Test 7: Brute-force AND with narrow-range second token';

-- 'broad' in all 4000 rows (many packed blocks).
-- 'spot' at rows 1999, 2000, 2001 (3 rows in a narrow range, likely embedded).
-- Brute-force: broad.linearOr fills bitmap, spot.linearAnd increments.
-- linearAndImpl for 'broad' processes only blocks near rows 1999-2001.

DROP TABLE IF EXISTS tab_skip7;

CREATE TABLE tab_skip7(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_skip7 SELECT number, concat('broad', if(number IN (1999, 2000, 2001), ' spot', '')) FROM numbers(4000);

SELECT count() FROM tab_skip7 WHERE hasAllTokens(s, ['broad', 'spot']);
SELECT k FROM tab_skip7 WHERE hasAllTokens(s, ['broad', 'spot']) ORDER BY k;

----------------------------------------------------
SELECT 'Test 8: Mixed embedded + large posting list in OR';

-- 'big' in all 2000 rows (large posting list, many blocks).
-- 'tiny' in only 2 rows (embedded posting list).
-- OR: should return 2000 rows (all of them, since 'big' covers everything).
-- This tests that linearOrImpl handles the case where it's called after
-- another cursor's linearOr has already set some bits.

DROP TABLE IF EXISTS tab_skip8;

CREATE TABLE tab_skip8(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_skip8 SELECT number, if(number IN (500, 1500), 'tiny', 'big') FROM numbers(2000);

-- OR: big(1998) + tiny(2) = 2000 total rows
SELECT count() FROM tab_skip8 WHERE hasAnyTokens(s, ['big', 'tiny']);

-- Verify tiny rows are found
SELECT k FROM tab_skip8 WHERE hasToken(s, 'tiny') ORDER BY k;

----------------------------------------------------
SELECT 'Test 9: Three-way AND with different densities across many blocks';

-- 'all' in all 3000 rows, 'half' in even rows (1500), 'quarter' in rows % 4 == 0 (750).
-- Three-way AND -> rows where all three match -> rows % 4 == 0 AND even -> rows % 4 == 0 -> 750.
-- All three tokens span many packed blocks.
-- Tests linearAnd block-skip with multiple tokens of different density.

DROP TABLE IF EXISTS tab_skip9;

CREATE TABLE tab_skip9(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_skip9 SELECT number,
    concat('all',
        if(number % 2 = 0, ' half', ''),
        if(number % 4 = 0, ' quarter', ''))
    FROM numbers(3000);

SELECT count() FROM tab_skip9 WHERE hasAllTokens(s, ['all', 'half', 'quarter']);
SELECT min(k), max(k) FROM tab_skip9 WHERE hasAllTokens(s, ['all', 'half', 'quarter']);

----------------------------------------------------
SELECT 'Test 10: Large posting list after merge — block skip still works';

-- Insert into 3 parts, merge, then query. Verifies that after merge the
-- Index Section is rebuilt correctly and block-skip optimization works.

DROP TABLE IF EXISTS tab_skip10;

CREATE TABLE tab_skip10(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_skip10 SELECT number, concat('merged', if(number % 100 = 0, ' century', '')) FROM numbers(1000);
INSERT INTO tab_skip10 SELECT number + 1000, concat('merged', if((number + 1000) % 100 = 0, ' century', '')) FROM numbers(1000);
INSERT INTO tab_skip10 SELECT number + 2000, concat('merged', if((number + 2000) % 100 = 0, ' century', '')) FROM numbers(1000);

OPTIMIZE TABLE tab_skip10 FINAL;

SELECT count() FROM tab_skip10 WHERE hasAllTokens(s, ['merged', 'century']);
SELECT min(k), max(k) FROM tab_skip10 WHERE hasAllTokens(s, ['merged', 'century']);

----------------------------------------------------
-- Cleanup
DROP TABLE IF EXISTS tab_skip1;
DROP TABLE IF EXISTS tab_skip2;
DROP TABLE IF EXISTS tab_skip3;
DROP TABLE IF EXISTS tab_skip4;
DROP TABLE IF EXISTS tab_skip5;
DROP TABLE IF EXISTS tab_skip6;
DROP TABLE IF EXISTS tab_skip7;
DROP TABLE IF EXISTS tab_skip8;
DROP TABLE IF EXISTS tab_skip9;
DROP TABLE IF EXISTS tab_skip10;
