DROP TABLE IF EXISTS t_text_index;

SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

CREATE TABLE t_text_index
(
    `id` UInt64,
    `s1` String,
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_text_index VALUES (1, 'A');

ALTER TABLE t_text_index ADD INDEX idx_text lower(s1) TYPE text(tokenizer = splitByNonAlpha);

SET enable_full_text_index = true;

INSERT INTO t_text_index VALUES (2, 'B');

OPTIMIZE TABLE t_text_index FINAL;

SELECT id FROM t_text_index WHERE hasAllTokens(lower(s1), 'a') ORDER BY id;
SELECT id FROM t_text_index WHERE hasAllTokens(lower(s1), 'b') ORDER BY id;

DROP TABLE IF EXISTS t_text_index;

CREATE TABLE t_text_index
(
    `id` UInt64,
    `s1` String,
    `s2` String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_text_index VALUES (1, 'A', 'B');

ALTER TABLE t_text_index ADD INDEX idx_text concat(s1, ' ', s2) TYPE text(tokenizer = splitByNonAlpha);

SET enable_full_text_index = true;

INSERT INTO t_text_index VALUES (2, 'C', 'D');

OPTIMIZE TABLE t_text_index FINAL;

SELECT id FROM t_text_index WHERE hasAllTokens(concat(s1, ' ', s2), 'A');
SELECT id FROM t_text_index WHERE hasAllTokens(concat(s1, ' ', s2), 'D');

DROP TABLE IF EXISTS t_text_index;
