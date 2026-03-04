-- Tags: no-fasttest
-- no-fasttest: It can be slow

-- Tests for v2 posting list format and lazy apply mode edge cases:
-- 1. Multiple large blocks per token (small posting_list_block_size)
-- 2. Seek across packed block boundaries (leapfrog intersection)
-- 3. Exact TurboPFor boundary doc counts (128, 256, 384 docs)
-- 4. Multi-way intersection (3, 4, 5+ tokens)
-- 5. High-density posting lists (brute-force path)
-- 6. first_doc_id prepend correctness
-- 7. Seek beyond all packed blocks (empty result)

SET enable_full_text_index = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;
SET use_skip_indexes_on_data_read = 0;

----------------------------------------------------
SELECT 'Test 1: Multiple large blocks with small posting_list_block_size=128';

-- posting_list_block_size=128 -> aligned to 128, so each large block holds exactly 128 docs.
-- A token appearing in 50% of 2000 rows -> 1000 docs -> ~8 large blocks.
-- This exercises multiple Index Sections and the cross-large-block iteration in PostingListCursor.

DROP TABLE IF EXISTS tab_multi_lb;

CREATE TABLE tab_multi_lb(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

-- 'common' appears in even rows (1000 times), 'uncommon' in odd rows (1000 times)
INSERT INTO tab_multi_lb SELECT number, if(number % 2 = 0, 'common', 'uncommon') FROM numbers(2000);

-- Verify count: 'common' should match exactly 1000 rows
SELECT count() FROM tab_multi_lb WHERE hasToken(s, 'common');

-- Verify count: 'uncommon' should also match exactly 1000 rows
SELECT count() FROM tab_multi_lb WHERE hasToken(s, 'uncommon');

-- AND: common + uncommon have zero overlap
SELECT count() FROM tab_multi_lb WHERE hasToken(s, 'common') AND hasToken(s, 'uncommon');

-- OR: common + uncommon = all rows
SELECT count() FROM tab_multi_lb WHERE hasAnyTokens(s, ['common', 'uncommon']);

----------------------------------------------------
SELECT 'Test 2: Seek across packed block boundaries with ngrams';

-- Use ngrams tokenizer so we can search with LIKE and arbitrary substrings.
-- Each row has a group label (ga/gb/gc/gd) appearing every 4 rows, and a frequency label.

DROP TABLE IF EXISTS tab_seek;

CREATE TABLE tab_seek(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = ngrams(3), posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

-- 'aaa' appears in 50% of rows (1000), 'bbb' in the other 50%
-- 'xxx' appears in 25% of rows (500), 'yyy' in another 25%
INSERT INTO tab_seek SELECT number,
    concat(if(number % 2 = 0, 'aaa', 'bbb'), ' ', if(number % 4 < 2, 'xxx', 'yyy'))
    FROM numbers(2000);

-- Two-way intersection: 'aaa' AND 'bbb' have no overlap
SELECT count() FROM tab_seek WHERE s LIKE '%aaa%' AND s LIKE '%bbb%';

-- Two-way intersection: 'aaa' AND 'xxx' -> even rows (aaa) AND rows 0,1,4,5,8,9,... (xxx)
-- aaa rows: 0,2,4,6,8,10,...  xxx rows: 0,1,4,5,8,9,...  intersection: 0,4,8,12,... = every 4th row = 500
SELECT count() FROM tab_seek WHERE s LIKE '%aaa%' AND s LIKE '%xxx%';

-- OR across large blocks: 'aaa' + 'bbb' = all 2000 rows
SELECT count() FROM tab_seek WHERE s LIKE '%aaa%' OR s LIKE '%bbb%';

----------------------------------------------------
SELECT 'Test 3: Exact TurboPFor boundary (128, 256, 384 docs)';

-- Test that tail_size=0 (perfectly aligned) works correctly.
-- With posting_list_block_size=128, insert exactly 129 rows (first_doc_id + 128 docs = 1 full packed block, no tail).

DROP TABLE IF EXISTS tab_boundary;

CREATE TABLE tab_boundary(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

-- All 129 rows have 'exact' token
INSERT INTO tab_boundary SELECT number, 'exact' FROM numbers(129);

SELECT count() FROM tab_boundary WHERE hasToken(s, 'exact');

-- 257 rows: first_doc_id + 256 docs = 2 full packed blocks, tail_size=0, one large block
DROP TABLE IF EXISTS tab_boundary2;

CREATE TABLE tab_boundary2(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 512))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_boundary2 SELECT number, 'exact' FROM numbers(257);

SELECT count() FROM tab_boundary2 WHERE hasToken(s, 'exact');

-- 385 rows: first_doc_id + 384 docs = 3 full packed blocks, tail_size=0
DROP TABLE IF EXISTS tab_boundary3;

CREATE TABLE tab_boundary3(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 512))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_boundary3 SELECT number, 'exact' FROM numbers(385);

SELECT count() FROM tab_boundary3 WHERE hasToken(s, 'exact');

-- Off-by-one: 130 rows = first_doc_id + 129 docs = 1 full block + tail of 1 doc
DROP TABLE IF EXISTS tab_boundary4;

CREATE TABLE tab_boundary4(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_boundary4 SELECT number, 'exact' FROM numbers(130);

SELECT count() FROM tab_boundary4 WHERE hasToken(s, 'exact');

----------------------------------------------------
SELECT 'Test 4: Multi-way intersection (3, 4, 5 tokens)';

-- Use a dataset where each row has multiple overlapping tokens so we can
-- construct 3/4/5-way AND intersections with non-empty results.
-- Use purely alphabetic tokens: pa/pb/pc/pd/pe + suffix digit spelled as letter

DROP TABLE IF EXISTS tab_multiway;

CREATE TABLE tab_multiway(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

-- Each row has 5 tokens derived from modular arithmetic:
-- pa{k%5} pb{k%7} pc{k%3} pd{k%11} pe{k%13}
-- Using array of spelled-out names to keep tokens alphabetic
INSERT INTO tab_multiway SELECT number,
    concat(
        arrayElement(['pazero','paone','patwo','pathree','pafour'], (number % 5) + 1), ' ',
        arrayElement(['pbzero','pbone','pbtwo','pbthree','pbfour','pbfive','pbsix'], (number % 7) + 1), ' ',
        arrayElement(['pczero','pcone','pctwo'], (number % 3) + 1), ' ',
        arrayElement(['pdzero','pdone','pdtwo','pdthree','pdfour','pdfive','pdsix','pdseven','pdeight','pdnine','pdten'], (number % 11) + 1), ' ',
        arrayElement(['pezero','peone','petwo','pethree','pefour','pefive','pesix','peseven','peeight','penine','peten','peeleven','petwelve'], (number % 13) + 1)
    )
    FROM numbers(3000);

-- 3-way AND: pazero AND pbzero AND pczero -> k % lcm(5,7,3) = k % 105 = 0 -> rows 0,105,...,2940 = 29 rows
SELECT count() FROM tab_multiway WHERE hasAllTokens(s, ['pazero', 'pbzero', 'pczero']);

-- 4-way AND: + pdzero -> k % lcm(5,7,3,11) = k % 1155 = 0 -> rows 0,1155,2310 = 3 rows
SELECT count() FROM tab_multiway WHERE hasAllTokens(s, ['pazero', 'pbzero', 'pczero', 'pdzero']);

-- Verify the actual k values of 4-way intersection
SELECT k FROM tab_multiway WHERE hasAllTokens(s, ['pazero', 'pbzero', 'pczero', 'pdzero']) ORDER BY k;

-- 5-way AND: + pezero -> k % lcm(5,7,3,11,13) = k % 15015 = 0 -> only row 0
SELECT count() FROM tab_multiway WHERE hasAllTokens(s, ['pazero', 'pbzero', 'pczero', 'pdzero', 'pezero']);

-- 5-way AND: verify exact row
SELECT k FROM tab_multiway WHERE hasAllTokens(s, ['pazero', 'pbzero', 'pczero', 'pdzero', 'pezero']);

----------------------------------------------------
SELECT 'Test 5: High-density posting lists (brute-force intersection path)';

-- Every row contains 'dense', so density=1.0, which should trigger intersectBruteForce.
-- Also add 'half' to 50% of rows, density~0.5 -> still above threshold.

DROP TABLE IF EXISTS tab_dense;

CREATE TABLE tab_dense(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_dense SELECT number,
    concat('dense ', if(number % 2 = 0, 'half', 'other'))
    FROM numbers(500);

-- AND of two high-density tokens: dense(100%) AND half(50%) -> half = 250 rows
SELECT count() FROM tab_dense WHERE hasAllTokens(s, ['dense', 'half']);

-- AND of dense AND other -> 250 rows (odd numbers)
SELECT count() FROM tab_dense WHERE hasAllTokens(s, ['dense', 'other']);

-- Verify: half AND other = 0 (mutually exclusive)
SELECT count() FROM tab_dense WHERE hasAllTokens(s, ['half', 'other']);

----------------------------------------------------
SELECT 'Test 6: first_doc_id prepend correctness';

-- The first doc_id is stored inline in the dictionary, not in TurboPFor data.
-- PostingListCursor must prepend it when decoding large block 0's packed block 0.
-- Verify the exact first row is returned correctly.

DROP TABLE IF EXISTS tab_first_doc;

CREATE TABLE tab_first_doc(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_first_doc SELECT number, 'target' FROM numbers(500);

-- 'target' appears in all 500 rows, first_doc_id=0
-- Seek to value 0 (the very first doc_id): must find it via prepend
SELECT min(k) FROM tab_first_doc WHERE hasToken(s, 'target');

-- Verify first and last row are both returned in linearOr
SELECT min(k), max(k) FROM tab_first_doc WHERE hasToken(s, 'target');

-- Verify total count matches
SELECT count() FROM tab_first_doc WHERE hasToken(s, 'target');

----------------------------------------------------
SELECT 'Test 7: Seek beyond all packed blocks (empty result)';

DROP TABLE IF EXISTS tab_empty_seek;

CREATE TABLE tab_empty_seek(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_empty_seek SELECT number, 'present' FROM numbers(500);

-- Search for non-existent token
SELECT count() FROM tab_empty_seek WHERE hasToken(s, 'nonexistent');

-- AND with non-existent token
SELECT count() FROM tab_empty_seek WHERE hasAllTokens(s, ['present', 'nonexistent']);

----------------------------------------------------
SELECT 'Test 8: Multiple large blocks after merge';

-- Merge two parts, each generating large posting lists. After merge, the merged
-- posting list should span even more large blocks with correct v2 Index Sections.

DROP TABLE IF EXISTS tab_merge_lb;

CREATE TABLE tab_merge_lb(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_merge_lb SELECT number, if(number % 2 = 0, 'common', 'uncommon') FROM numbers(1000);
INSERT INTO tab_merge_lb SELECT number + 1000, if((number + 1000) % 2 = 0, 'common', 'uncommon') FROM numbers(1000);

OPTIMIZE TABLE tab_merge_lb FINAL;

SELECT count() FROM tab_merge_lb WHERE hasToken(s, 'common');

SELECT count() FROM tab_merge_lb WHERE hasToken(s, 'uncommon');

-- AND after merge: common + uncommon = 0 overlap
SELECT count() FROM tab_merge_lb WHERE hasToken(s, 'common') AND hasToken(s, 'uncommon');

----------------------------------------------------
SELECT 'Test 9: Seek within and across large blocks with ngrams';

-- Use ngrams tokenizer for targeted seek via LIKE on per-row unique substrings.
-- 'freq' appears in every row -> 1500 docs -> many large blocks of 128

DROP TABLE IF EXISTS tab_seek_lb;

CREATE TABLE tab_seek_lb(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = ngrams(4), posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_seek_lb SELECT number, concat('freq zz', leftPad(toString(number), 5, '0')) FROM numbers(1500);

-- Seek to a doc in the middle via LIKE
SELECT k FROM tab_seek_lb WHERE s LIKE '%freq%' AND s LIKE '%zz00750%';

-- Seek to last doc
SELECT k FROM tab_seek_lb WHERE s LIKE '%freq%' AND s LIKE '%zz01499%';

-- Seek to first doc
SELECT k FROM tab_seek_lb WHERE s LIKE '%freq%' AND s LIKE '%zz00000%';

----------------------------------------------------
SELECT 'Test 10: Direct read mode with v2 format and large posting list';

DROP TABLE IF EXISTS tab_dr_v2;

CREATE TABLE tab_dr_v2(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_dr_v2 SELECT number, if(number % 3 = 0, 'common', 'other') FROM numbers(600);

SET query_plan_direct_read_from_text_index = 1;

SELECT count() FROM tab_dr_v2 WHERE hasToken(s, 'common');

-- AND: common + other = 0 (disjoint)
SELECT count() FROM tab_dr_v2 WHERE hasToken(s, 'common') AND hasToken(s, 'other');

SET query_plan_direct_read_from_text_index = 0;

----------------------------------------------------
DROP TABLE IF EXISTS tab_multi_lb;
DROP TABLE IF EXISTS tab_seek;
DROP TABLE IF EXISTS tab_boundary;
DROP TABLE IF EXISTS tab_boundary2;
DROP TABLE IF EXISTS tab_boundary3;
DROP TABLE IF EXISTS tab_boundary4;
DROP TABLE IF EXISTS tab_multiway;
DROP TABLE IF EXISTS tab_dense;
DROP TABLE IF EXISTS tab_first_doc;
DROP TABLE IF EXISTS tab_empty_seek;
DROP TABLE IF EXISTS tab_merge_lb;
DROP TABLE IF EXISTS tab_seek_lb;
DROP TABLE IF EXISTS tab_dr_v2;
