-- Test for Bug 86300
-- This is different from the empty needle [''] which should match nothing
-- See: 02346_text_index_functions_with_empty_needle.reference

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (
    id Int,
    text String,
    INDEX idx_text(text) TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE=MergeTree()
ORDER BY (id);

INSERT INTO tab VALUES(1, 'bar'), (2, 'foo');

-- No needles means no filtering --> match every row
SELECT count() FROM tab WHERE hasAnyTokens(text, []);
SELECT count() FROM tab WHERE hasAllTokens(text, []);

DROP TABLE tab;
