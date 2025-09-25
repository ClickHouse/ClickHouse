-- Test for Bug 86300

SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

SELECT 'Match every row for empty needles with text Index';

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

SELECT count() FROM tab WHERE searchAny(text, ['']);
SELECT count() FROM tab WHERE searchAll(text, ['']);
SELECT count() FROM tab WHERE hasToken(text, '');

select '---';

SELECT count() FROM tab WHERE NOT searchAny(text, []);
SELECT count() FROM tab WHERE NOT searchAll(text, []);
SELECT count() FROM tab WHERE NOT searchAny(text, ['']);
SELECT count() FROM tab WHERE NOT searchAll(text, ['']);
SELECT count() FROM tab WHERE NOT hasToken(text, '');

SELECT 'Match every row for empty needles without text Index';
SET use_skip_indexes_on_data_read = 0;

SELECT count() FROM tab WHERE searchAny(text, []);
SELECT count() FROM tab WHERE searchAll(text, []);

SELECT count() FROM tab WHERE searchAny(text, ['']);
SELECT count() FROM tab WHERE searchAll(text, ['']);
SELECT count() FROM tab WHERE hasToken(text, '');

select '---';

SELECT count() FROM tab WHERE NOT searchAny(text, []);
SELECT count() FROM tab WHERE NOT searchAll(text, []);
SELECT count() FROM tab WHERE NOT searchAny(text, ['']);
SELECT count() FROM tab WHERE NOT searchAll(text, ['']);
SELECT count() FROM tab WHERE NOT hasToken(text, '');

DROP TABLE tab;
