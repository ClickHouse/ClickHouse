DROP TABLE IF EXISTS tab;

-- Tests vertical merges for columns with text index

SET mutations_sync = 2;
SET use_skip_indexes_on_data_read = 1;

CREATE TABLE tab
(
    id UInt64,
    c1 String,
    c2 String
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO tab SELECT number, 'c1' || toString(number), 'c2' || toString(number + 1) FROM numbers(10000);

ALTER TABLE tab ADD INDEX idx_c1 (c1) TYPE text (tokenizer = ngrams(3));
ALTER TABLE tab ADD INDEX idx_c2 (c2) TYPE text (tokenizer = ngrams(3));

INSERT INTO tab SELECT number, 'c1' || toString(number), 'c2' || toString(number + 1) FROM numbers(10000);

SELECT count() FROM tab WHERE hasAllTokens(c1, 'c11') AND hasAllTokens(c2, 'c21');

OPTIMIZE TABLE tab FINAL;

SELECT count() FROM tab WHERE hasAllTokens(c1, 'c11') AND hasAllTokens(c2, 'c21');

DROP TABLE tab;
