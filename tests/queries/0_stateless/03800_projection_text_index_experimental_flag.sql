-- Tests that CREATE TABLE and ADD INDEX respect settings 'enable_full_text_index'

DROP TABLE IF EXISTS tab;

-- Test CREATE TABLE

SET enable_full_text_index = 0;
CREATE TABLE tab1 (id UInt32, str String, PROJECTION idx INDEX str TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError SUPPORT_IS_DISABLED }

SET enable_full_text_index = 1;
CREATE TABLE tab1 (id UInt32, str String, PROJECTION idx INDEX str TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tab1;

-- Test ADD INDEX

SET enable_full_text_index = 0;
CREATE TABLE tab (id UInt32, str String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE tab ADD PROJECTION idx1 INDEX str TYPE text(tokenizer = 'splitByNonAlpha');  -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE tab;

SET enable_full_text_index = 1;
CREATE TABLE tab (id UInt32, str String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE tab ADD PROJECTION idx1 INDEX str TYPE text(tokenizer = 'splitByNonAlpha');
DROP TABLE tab;
