SET enable_full_text_index = 1;
SET mutations_sync = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS t_multi_text_idx_lwd_vertical;

CREATE TABLE t_multi_text_idx_lwd_vertical
(
    id UInt64,
    c1 String,
    c2 String
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

-- Insert data, add text indexes, then do LWD + more inserts + optimize
INSERT INTO t_multi_text_idx_lwd_vertical SELECT number, 'c1_' || toString(number), 'c2_' || toString(number + 1) FROM numbers(1000);

ALTER TABLE t_multi_text_idx_lwd_vertical ADD INDEX idx_c1 (c1) TYPE text(tokenizer = ngrams(3));
ALTER TABLE t_multi_text_idx_lwd_vertical ADD INDEX idx_c2 (c2) TYPE text(tokenizer = ngrams(3));

-- Second insert (these parts have the text indexes)
INSERT INTO t_multi_text_idx_lwd_vertical SELECT number + 1000, 'c1_' || toString(number + 1000), 'c2_' || toString(number + 1001) FROM numbers(1000);

-- LWD to trigger merge_may_reduce_rows
DELETE FROM t_multi_text_idx_lwd_vertical WHERE id % 10 = 0;

-- Insert more data
INSERT INTO t_multi_text_idx_lwd_vertical SELECT number + 2000, 'c1_' || toString(number + 2000), 'c2_' || toString(number + 2001) FROM numbers(1000);

-- Count before optimize
SELECT count() FROM t_multi_text_idx_lwd_vertical WHERE hasAllTokens(c1, 'c1_15') AND hasAllTokens(c2, 'c2_16') ORDER BY 1;

-- Force merge (vertical merge + LWD + multiple text indexes)
OPTIMIZE TABLE t_multi_text_idx_lwd_vertical FINAL;

-- Count after optimize - should be same
SELECT count() FROM t_multi_text_idx_lwd_vertical WHERE hasAllTokens(c1, 'c1_15') AND hasAllTokens(c2, 'c2_16') ORDER BY 1;

DROP TABLE IF EXISTS t_multi_text_idx_lwd_vertical;
