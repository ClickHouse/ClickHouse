-- Tags: no-fasttest
-- Reproduces a bug where MergeTreeReaderTextIndex::readRows overruns past the
-- final mark when the text index reader is non-first in the reader chain
-- (pushed there by a prepared_index reader created from a minmax skip index).
--
-- The root cause: each batch boundary that falls mid-mark causes the text index
-- reader's internal current_row to drift behind getMarkStartingRow(current_mark).
-- After enough batches, max_rows_to_read exceeds remaining mark rows, the while
-- loop hits the final mark (0 rows), makes no progress, and increments past the
-- end -> "Trying to get non existing mark N, while size is N".

DROP TABLE IF EXISTS t_text_index_skip_bug;

CREATE TABLE t_text_index_skip_bug
(
    id UInt64,
    body String,
    created_at DateTime,
    INDEX fts_body body TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1,
    INDEX idx_minmax created_at TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 8192,
    index_granularity_bytes = '10M',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

INSERT INTO t_text_index_skip_bug
SELECT
    number,
    concat('document ', toString(number), if(number % 100 = 0, ' vector', '')),
    toDateTime('2024-01-01 00:00:00') - toIntervalSecond(number)
FROM numbers(200000);

OPTIMIZE TABLE t_text_index_skip_bug FINAL;

-- Full scan that reaches the end of the part.
-- Without the fix this crashes with "Trying to get non existing mark 26, while size is 26".
SELECT count()
FROM t_text_index_skip_bug
WHERE hasToken(body, 'vector') AND created_at >= (toDateTime('2024-01-01 00:00:00') - toIntervalMonth(1))
SETTINGS
    enable_full_text_index = 1,
    use_skip_indexes = 1,
    use_query_condition_cache = 0,
    query_plan_direct_read_from_text_index = 1,
    use_skip_indexes_on_data_read = 1,
    max_threads = 1,
    max_block_size = 65505;

DROP TABLE t_text_index_skip_bug;
