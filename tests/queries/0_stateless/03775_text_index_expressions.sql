DROP TABLE IF EXISTS kek;

SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

CREATE TABLE kek
(
    `id` UInt64,
    `s1` String,
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO kek VALUES (1, 'A');

ALTER TABLE kek ADD INDEX idx_text lower(s1) TYPE text(tokenizer = splitByNonAlpha);

SET enable_full_text_index = true;

INSERT INTO kek VALUES (2, 'B');

OPTIMIZE TABLE kek FINAL;

SELECT id FROM kek WHERE hasAllTokens(lower(s1), 'a') ORDER BY id;
SELECT id FROM kek WHERE hasAllTokens(lower(s1), 'b') ORDER BY id;

DROP TABLE IF EXISTS kek;

CREATE TABLE kek
(
    `id` UInt64,
    `s1` String,
    `s2` String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO kek VALUES (1, 'A', 'B');

ALTER TABLE kek ADD INDEX idx_text concat(s1, ' ', s2) TYPE text(tokenizer = splitByNonAlpha);

SET enable_full_text_index = true;

INSERT INTO kek VALUES (2, 'C', 'D');

OPTIMIZE TABLE kek FINAL;

SELECT id FROM kek WHERE hasAllTokens(concat(s1, ' ', s2), 'A');
SELECT id FROM kek WHERE hasAllTokens(concat(s1, ' ', s2), 'D');

DROP TABLE IF EXISTS kek;
