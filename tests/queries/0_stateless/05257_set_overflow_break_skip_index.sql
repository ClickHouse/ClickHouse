-- A set cut short by `set_overflow_mode = 'break'` still holds every row it managed to read, so a skip
-- index built from that set has to consider all of them. When the rows of the block that crossed the
-- limit are missing from the stored set, the index prunes granules the query matches, and reading the
-- set past its end aborts the server.

DROP TABLE IF EXISTS t_set_break_skip_index;

CREATE TABLE t_set_break_skip_index
(
    id UInt64,
    s String,
    INDEX idx_tokenbf s TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1,
    INDEX idx_text s TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO t_set_break_skip_index SELECT number, 'word' || toString(number) FROM numbers(64);

SELECT '-- both indexes answer an IN over a subquery';
SELECT count() FROM t_set_break_skip_index WHERE s IN (SELECT 'word5')
SETTINGS force_data_skipping_indices = 'idx_tokenbf,idx_text';

SELECT '-- a truncated set: with and without the skip indexes must agree';
SELECT count() FROM t_set_break_skip_index
WHERE s IN (SELECT 'word' || toString(number) FROM numbers(64))
SETTINGS max_rows_in_set = 5, set_overflow_mode = 'break', max_block_size = 4, max_threads = 1,
         use_index_for_in_with_subqueries = 1, use_skip_indexes = 1,
         force_data_skipping_indices = 'idx_tokenbf,idx_text';

SELECT count() FROM t_set_break_skip_index
WHERE s IN (SELECT 'word' || toString(number) FROM numbers(64))
SETTINGS max_rows_in_set = 5, set_overflow_mode = 'break', max_block_size = 4, max_threads = 1,
         use_index_for_in_with_subqueries = 1, use_skip_indexes = 0;

DROP TABLE t_set_break_skip_index;
