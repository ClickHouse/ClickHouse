-- Test the behavior of text index functions with empty needle
-- They should not match anything

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (
    id Int,
    text String,
    INDEX idx_text(text) TYPE text(tokenizer = 'default')
)
ENGINE = MergeTree()
ORDER BY (id);

INSERT INTO tab VALUES(1, 'bar'), (2, 'foo');

SELECT '-- Plain text index search functions';
SELECT count() FROM tab WHERE searchAny(text, ['']);
SELECT count() FROM tab WHERE searchAll(text, ['']);
SELECT count() FROM tab WHERE hasToken(text, '');

SELECT '-- Negated text index search functions';
SELECT count() FROM tab WHERE NOT searchAny(text, ['']);
SELECT count() FROM tab WHERE NOT searchAll(text, ['']);
SELECT count() FROM tab WHERE NOT hasToken(text, '');

DROP TABLE tab;
