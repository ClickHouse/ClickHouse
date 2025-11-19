-- Test for Bug 86300

SET allow_experimental_full_text_index = 1;

SELECT 'Match every row for empty needles';

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (
    id Int,
    text String,
    INDEX idx_text(text) TYPE text(tokenizer = 'default')
)
ENGINE=MergeTree()
ORDER BY (id);

INSERT INTO tab VALUES(1, 'bar'), (2, 'foo');

SELECT count() FROM tab WHERE searchAny(text, []);
SELECT count() FROM tab WHERE searchAll(text, []);

DROP TABLE tab;
