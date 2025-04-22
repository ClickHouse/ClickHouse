-- Tests that CREATE TABLE and ADD INDEX respect settings 'allow_experimental_full_text_index' and `allow_experimental_inverted_index`

DROP TABLE IF EXISTS tab;

-- Test CREATE TABLE + full_text index setting

SET allow_experimental_full_text_index = 0;
CREATE TABLE tab1 (id UInt32, str String, INDEX idx str TYPE gin(0)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE tab2 (id UInt32, str String, INDEX idx str TYPE full_text(0)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_INDEX }
CREATE TABLE tab3 (id UInt32, str String, INDEX idx str TYPE inverted(0)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_INDEX }

SET allow_experimental_full_text_index = 1;
CREATE TABLE tab1 (id UInt32, str String, INDEX idx str TYPE gin(0)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tab2 (id UInt32, str String, INDEX idx str TYPE full_text(0)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tab3 (id UInt32, str String, INDEX idx str TYPE inverted(0)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_INDEX }
DROP TABLE tab1, tab2;

SET allow_experimental_full_text_index = 0; -- reset to default

-- Test CREATE TABLE + inverted index setting

SET allow_experimental_inverted_index = 0;
CREATE TABLE tab1 (id UInt32, str String, INDEX idx str TYPE gin(0)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE tab2 (id UInt32, str String, INDEX idx str TYPE full_text(0)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_INDEX }
CREATE TABLE tab3 (id UInt32, str String, INDEX idx str TYPE inverted(0)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_INDEX }

SET allow_experimental_inverted_index = 1;
CREATE TABLE tab1 (id UInt32, str String, INDEX idx str TYPE gin(0)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE tab2 (id UInt32, str String, INDEX idx str TYPE full_text(0)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_INDEX }
CREATE TABLE tab3 (id UInt32, str String, INDEX idx str TYPE inverted(0)) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tab3;


SET allow_experimental_inverted_index = 0; -- reset to default

-- Test ADD INDEX + full_text index setting

SET allow_experimental_full_text_index = 0;
CREATE TABLE tab (id UInt32, str String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE tab ADD INDEX idx1 str TYPE gin(0);  -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE tab ADD INDEX idx2 str TYPE full_text(0);  -- { serverError ILLEGAL_INDEX }
ALTER TABLE tab ADD INDEX idx3 str TYPE inverted(0); -- { serverError ILLEGAL_INDEX }
DROP TABLE tab;

SET allow_experimental_full_text_index = 1;
CREATE TABLE tab (id UInt32, str String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE tab ADD INDEX idx1 str TYPE gin(0);
ALTER TABLE tab ADD INDEX idx2 str TYPE full_text(0);
ALTER TABLE tab ADD INDEX idx3 str TYPE inverted(0); -- { serverError ILLEGAL_INDEX }
DROP TABLE tab;
SET allow_experimental_full_text_index = 0; -- reset to default


-- Test ADD INDEX + inverted index setting

SET allow_experimental_inverted_index = 0;
CREATE TABLE tab (id UInt32, str String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE tab ADD INDEX idx1 str TYPE gin(0);  -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE tab ADD INDEX idx2 str TYPE full_text(0);  -- { serverError ILLEGAL_INDEX }
ALTER TABLE tab ADD INDEX idx3 str TYPE inverted(0); -- { serverError ILLEGAL_INDEX }
DROP TABLE tab;

SET allow_experimental_inverted_index = 1;
CREATE TABLE tab (id UInt32, str String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE tab ADD INDEX idx1 str TYPE gin(0); -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE tab ADD INDEX idx2 str TYPE full_text(0); -- { serverError ILLEGAL_INDEX }
ALTER TABLE tab ADD INDEX idx3 str TYPE inverted(0);
DROP TABLE tab;
SET allow_experimental_inverted_index = 0; -- reset to default
