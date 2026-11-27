SET use_skip_indexes_on_data_read = 1;

-- Tests for text indexes build on expressions

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    s1 String,
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'A');

ALTER TABLE tab ADD INDEX idx_text lower(s1) TYPE text(tokenizer = splitByNonAlpha);

INSERT INTO tab VALUES (2, 'B');

OPTIMIZE TABLE tab FINAL;

SELECT id FROM tab WHERE hasAllTokens(lower(s1), 'a') ORDER BY id;
SELECT id FROM tab WHERE hasAllTokens(lower(s1), 'b') ORDER BY id;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    s1 String,
    s2 String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'A', 'B');

ALTER TABLE tab ADD INDEX idx_text concat(s1, ' ', s2) TYPE text(tokenizer = splitByNonAlpha);

INSERT INTO tab VALUES (2, 'C', 'D');

OPTIMIZE TABLE tab FINAL;

SELECT id FROM tab WHERE hasAllTokens(concat(s1, ' ', s2), 'A');
SELECT id FROM tab WHERE hasAllTokens(concat(s1, ' ', s2), 'D');

DROP TABLE IF EXISTS tab;
