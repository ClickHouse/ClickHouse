-- Test the behavior of text index functions with empty needle
-- They should not match anything

-- In search{All,Any} empty needle is different from empty list:
-- See: 02346_text_index_bug86300

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (
    id Int,
    text String,
    INDEX idx_text(text) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree()
ORDER BY (id);

INSERT INTO tab VALUES(1, 'bar'), (2, 'foo');

SELECT '-- Plain text index search functions';
SELECT count() FROM tab WHERE hasAnyTokens(text, ['']);
SELECT count() FROM tab WHERE hasAllTokens(text, ['']);
SELECT count() FROM tab WHERE hasToken(text, '');

SELECT '-- Negated text index search functions';
SELECT count() FROM tab WHERE NOT hasAnyTokens(text, ['']);
SELECT count() FROM tab WHERE NOT hasAllTokens(text, ['']);
SELECT count() FROM tab WHERE NOT hasToken(text, '');

DROP TABLE tab;
