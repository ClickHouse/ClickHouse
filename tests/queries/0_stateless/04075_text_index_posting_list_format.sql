-- Tags: no-fasttest
-- no-fasttest: It can be slow

-- Tests for posting list format validation:
-- 1. posting_list_block_size controls segment granularity (multi-segment after merge)
-- 2. Invalid DDL parameter (unknown param) is rejected
-- 3. Invalid DDL parameter posting_list_version is rejected

SET enable_full_text_index = 1;
SET allow_experimental_text_index_lazy_apply = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

----------------------------------------------------
SELECT 'Test 1: posting_list_block_size controls segment granularity';

-- Small block size forces multiple segments per token.
-- After merge the segments should be rebuilt correctly.

DROP TABLE IF EXISTS tab_seg;

CREATE TABLE tab_seg(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_seg SELECT number, if(number % 2 = 0, 'common', 'rare') FROM numbers(1000);
INSERT INTO tab_seg SELECT number + 1000, if((number + 1000) % 2 = 0, 'common', 'rare') FROM numbers(1000);

OPTIMIZE TABLE tab_seg FINAL;

SET text_index_posting_list_apply_mode = 'materialize';
SELECT count() FROM tab_seg WHERE hasToken(s, 'common');
SELECT count() FROM tab_seg WHERE hasToken(s, 'rare');
SELECT count() FROM tab_seg WHERE hasToken(s, 'common') AND hasToken(s, 'rare');

----------------------------------------------------
SELECT 'Test 2: Invalid DDL parameter is rejected';

DROP TABLE IF EXISTS tab_invalid_param;
CREATE TABLE tab_invalid_param(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', has_block_index = 1))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi'; -- { serverError BAD_ARGUMENTS }

----------------------------------------------------
SELECT 'Test 3: Invalid DDL parameter posting_list_version is rejected';

DROP TABLE IF EXISTS tab_invalid;
CREATE TABLE tab_invalid(k UInt64, s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_version = 2))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi'; -- { serverError BAD_ARGUMENTS }

----------------------------------------------------
DROP TABLE IF EXISTS tab_seg;
DROP TABLE IF EXISTS tab_invalid_param;
DROP TABLE IF EXISTS tab_invalid;
