SET allow_experimental_projection_text_index = 1;

-- Tests for posting list format and has_block_index DDL parameter:
-- 1. Default format with small data (block index enabled by default, lazy mode)
-- 2. Default format with large data (triggers large posting list path, lazy)
-- 3. Large data after merge (block index preserved)
-- 4. Explicit has_block_index=1 (same as default)
-- 5. has_block_index=0 (no block index, non-lazy apply mode)
-- 6. has_block_index=0 with large data after merge
-- 7. Invalid has_block_index=2 is rejected
-- 8. Invalid DDL parameter posting_list_version is rejected

SET enable_full_text_index = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;
SET use_skip_indexes_on_data_read = 0;

----------------------------------------------------
SELECT 'Test 1: Default format (auto lazy mode)';

DROP TABLE IF EXISTS tab_default;

CREATE TABLE tab_default(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha'))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_default SELECT number, if(number % 2 = 0, 'alpha', 'beta') FROM numbers(200);

SELECT count() FROM tab_default WHERE hasToken(s, 'alpha');
SELECT count() FROM tab_default WHERE hasToken(s, 'beta');
SELECT count() FROM tab_default WHERE hasToken(s, 'alpha') AND hasToken(s, 'beta');

----------------------------------------------------
SELECT 'Test 2: Large data with default format (lazy, triggers large posting list)';

DROP TABLE IF EXISTS tab_large;

CREATE TABLE tab_large(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_large SELECT number, if(number % 2 = 0, 'common', 'rare') FROM numbers(2000);

SELECT count() FROM tab_large WHERE hasToken(s, 'common');
SELECT count() FROM tab_large WHERE hasToken(s, 'rare');
SELECT count() FROM tab_large WHERE hasToken(s, 'common') AND hasToken(s, 'rare');

----------------------------------------------------
SELECT 'Test 3: Large data after merge (block index preserved)';

DROP TABLE IF EXISTS tab_merge;

CREATE TABLE tab_merge(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_merge SELECT number, if(number % 2 = 0, 'common', 'rare') FROM numbers(1000);
INSERT INTO tab_merge SELECT number + 1000, if((number + 1000) % 2 = 0, 'common', 'rare') FROM numbers(1000);

OPTIMIZE TABLE tab_merge FINAL;

SELECT count() FROM tab_merge WHERE hasToken(s, 'common');
SELECT count() FROM tab_merge WHERE hasToken(s, 'rare');

----------------------------------------------------
SELECT 'Test 4: Explicit has_block_index=1 (same as default)';

DROP TABLE IF EXISTS tab_explicit_bi;

CREATE TABLE tab_explicit_bi(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', has_block_index = 1, posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_explicit_bi SELECT number, if(number % 2 = 0, 'common', 'rare') FROM numbers(2000);

SELECT count() FROM tab_explicit_bi WHERE hasToken(s, 'common');
SELECT count() FROM tab_explicit_bi WHERE hasToken(s, 'rare');
SELECT count() FROM tab_explicit_bi WHERE hasToken(s, 'common') AND hasToken(s, 'rare');

----------------------------------------------------
SELECT 'Test 5: has_block_index=0 (no block index, non-lazy apply mode)';

DROP TABLE IF EXISTS tab_no_bi;

CREATE TABLE tab_no_bi(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', has_block_index = 0, posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_no_bi SELECT number, if(number % 2 = 0, 'common', 'rare') FROM numbers(2000);

SELECT count() FROM tab_no_bi WHERE hasToken(s, 'common');
SELECT count() FROM tab_no_bi WHERE hasToken(s, 'rare');
SELECT count() FROM tab_no_bi WHERE hasToken(s, 'common') AND hasToken(s, 'rare');

----------------------------------------------------
SELECT 'Test 6: has_block_index=0 with large data after merge';

DROP TABLE IF EXISTS tab_no_bi_merge;

CREATE TABLE tab_no_bi_merge(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', has_block_index = 0, posting_list_block_size = 128))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_no_bi_merge SELECT number, if(number % 2 = 0, 'common', 'rare') FROM numbers(1000);
INSERT INTO tab_no_bi_merge SELECT number + 1000, if((number + 1000) % 2 = 0, 'common', 'rare') FROM numbers(1000);

OPTIMIZE TABLE tab_no_bi_merge FINAL;

SELECT count() FROM tab_no_bi_merge WHERE hasToken(s, 'common');
SELECT count() FROM tab_no_bi_merge WHERE hasToken(s, 'rare');

----------------------------------------------------
SELECT 'Test 7: Invalid has_block_index=2 is rejected';

DROP TABLE IF EXISTS tab_invalid_bi;
CREATE TABLE tab_invalid_bi(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', has_block_index = 2))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi'; -- { serverError BAD_ARGUMENTS }

----------------------------------------------------
SELECT 'Test 8: Invalid DDL parameter posting_list_version is rejected';

DROP TABLE IF EXISTS tab_invalid;
CREATE TABLE tab_invalid(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_version = 2))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi'; -- { serverError BAD_ARGUMENTS }

----------------------------------------------------
DROP TABLE IF EXISTS tab_default;
DROP TABLE IF EXISTS tab_large;
DROP TABLE IF EXISTS tab_merge;
DROP TABLE IF EXISTS tab_explicit_bi;
DROP TABLE IF EXISTS tab_no_bi;
DROP TABLE IF EXISTS tab_no_bi_merge;
DROP TABLE IF EXISTS tab_invalid_bi;
DROP TABLE IF EXISTS tab_invalid;
