-- Tags: no-fasttest
-- no-fasttest: It can be slow

-- Tests for posting_list_version DDL parameter:
-- 1. posting_list_version=1 + materialize mode (auto) -> normal query
-- 2. posting_list_version=2 + lazy mode (auto) -> normal query
-- 3. Default (not specified) -> uses v2, lazy works
-- 4. Large data test with v1 (triggers large posting list path, materialize)
-- 5. Large data test with v2 (triggers large posting list path, lazy)
-- 6. Large data with v2 after merge
-- 7. Invalid posting_list_version

SET enable_full_text_index = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;
SET use_skip_indexes_on_data_read = 0;

----------------------------------------------------
SELECT 'Test 1: posting_list_version=1 (auto materialize mode)';

DROP TABLE IF EXISTS tab_v1;

CREATE TABLE tab_v1(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_version = 1))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_v1 SELECT number, if(number % 2 = 0, 'alpha', 'beta') FROM numbers(200);

SELECT count() FROM tab_v1 WHERE hasToken(s, 'alpha');
SELECT count() FROM tab_v1 WHERE hasToken(s, 'beta');
SELECT count() FROM tab_v1 WHERE hasToken(s, 'alpha') AND hasToken(s, 'beta');

----------------------------------------------------
SELECT 'Test 2: posting_list_version=2 (auto lazy mode)';

DROP TABLE IF EXISTS tab_v2;

CREATE TABLE tab_v2(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_version = 2))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_v2 SELECT number, if(number % 2 = 0, 'alpha', 'beta') FROM numbers(200);

SELECT count() FROM tab_v2 WHERE hasToken(s, 'alpha');
SELECT count() FROM tab_v2 WHERE hasToken(s, 'beta');
SELECT count() FROM tab_v2 WHERE hasToken(s, 'alpha') AND hasToken(s, 'beta');

----------------------------------------------------
SELECT 'Test 3: Default posting_list_version (v2, auto lazy)';

DROP TABLE IF EXISTS tab_default;

CREATE TABLE tab_default(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha'))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_default SELECT number, if(number % 3 = 0, 'gamma', 'delta') FROM numbers(300);

SELECT count() FROM tab_default WHERE hasToken(s, 'gamma');
SELECT count() FROM tab_default WHERE hasToken(s, 'delta');

----------------------------------------------------
SELECT 'Test 4: Large data with posting_list_version=1 (materialize, triggers large posting list)';

DROP TABLE IF EXISTS tab_v1_large;

CREATE TABLE tab_v1_large(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128, posting_list_version = 1))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_v1_large SELECT number, if(number % 2 = 0, 'common', 'rare') FROM numbers(2000);

SELECT count() FROM tab_v1_large WHERE hasToken(s, 'common');
SELECT count() FROM tab_v1_large WHERE hasToken(s, 'rare');
SELECT count() FROM tab_v1_large WHERE hasToken(s, 'common') AND hasToken(s, 'rare');

----------------------------------------------------
SELECT 'Test 5: Large data with posting_list_version=2 (lazy)';

DROP TABLE IF EXISTS tab_v2_large;

CREATE TABLE tab_v2_large(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128, posting_list_version = 2))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_v2_large SELECT number, if(number % 2 = 0, 'common', 'rare') FROM numbers(2000);

SELECT count() FROM tab_v2_large WHERE hasToken(s, 'common');
SELECT count() FROM tab_v2_large WHERE hasToken(s, 'rare');
SELECT count() FROM tab_v2_large WHERE hasToken(s, 'common') AND hasToken(s, 'rare');

----------------------------------------------------
SELECT 'Test 6: Large data with posting_list_version=2 after merge';

DROP TABLE IF EXISTS tab_v2_merge;

CREATE TABLE tab_v2_merge(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 128, posting_list_version = 2))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO tab_v2_merge SELECT number, if(number % 2 = 0, 'common', 'rare') FROM numbers(1000);
INSERT INTO tab_v2_merge SELECT number + 1000, if((number + 1000) % 2 = 0, 'common', 'rare') FROM numbers(1000);

OPTIMIZE TABLE tab_v2_merge FINAL;

SELECT count() FROM tab_v2_merge WHERE hasToken(s, 'common');
SELECT count() FROM tab_v2_merge WHERE hasToken(s, 'rare');

----------------------------------------------------
SELECT 'Test 7: Invalid posting_list_version';

DROP TABLE IF EXISTS tab_invalid;
CREATE TABLE tab_invalid(k UInt64, s String, PROJECTION af INDEX s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_version = 3))
    ENGINE = MergeTree() ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi'; -- { serverError BAD_ARGUMENTS }

----------------------------------------------------
DROP TABLE IF EXISTS tab_v1;
DROP TABLE IF EXISTS tab_v2;
DROP TABLE IF EXISTS tab_default;
DROP TABLE IF EXISTS tab_v1_large;
DROP TABLE IF EXISTS tab_v2_large;
DROP TABLE IF EXISTS tab_v2_merge;
DROP TABLE IF EXISTS tab_invalid;
