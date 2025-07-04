-- Tests that CREATE TABLE and ADD INDEX respect settings 'allow_experimental_full_text_index' and `allow_experimental_inverted_index`

DROP TABLE IF EXISTS tab;

-- Test CREATE TABLE + full_text index setting

SET allow_experimental_full_text_index = 0;
CREATE TABLE tab1 (id UInt32, str String, INDEX idx str TYPE text(tokenizer = 'default')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_full_text_index = 1;
CREATE TABLE tab1 (id UInt32, str String, INDEX idx str TYPE text(tokenizer = 'default')) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tab1;

SET allow_experimental_full_text_index = 0; -- reset to default

-- Test CREATE TABLE + inverted index setting

SET allow_experimental_inverted_index = 0;
CREATE TABLE tab1 (id UInt32, str String, INDEX idx str TYPE text(tokenizer = 'default')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_inverted_index = 1;
CREATE TABLE tab1 (id UInt32, str String, INDEX idx str TYPE text(tokenizer = 'default')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError SUPPORT_IS_DISABLED }


SET allow_experimental_inverted_index = 0; -- reset to default

-- Test ADD INDEX + full_text index setting

SET allow_experimental_full_text_index = 0;
CREATE TABLE tab (id UInt32, str String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE tab ADD INDEX idx1 str TYPE text(tokenizer = 'default');  -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE tab;

SET allow_experimental_full_text_index = 1;
CREATE TABLE tab (id UInt32, str String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE tab ADD INDEX idx1 str TYPE text(tokenizer = 'default');
DROP TABLE tab;
SET allow_experimental_full_text_index = 0; -- reset to default


-- Test ADD INDEX + inverted index setting

SET allow_experimental_inverted_index = 0;
CREATE TABLE tab (id UInt32, str String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE tab ADD INDEX idx1 str TYPE text(tokenizer = 'default');  -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE tab;

SET allow_experimental_inverted_index = 1;
CREATE TABLE tab (id UInt32, str String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE tab ADD INDEX idx1 str TYPE text(tokenizer = 'default'); -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE tab;
SET allow_experimental_inverted_index = 0; -- reset to default
