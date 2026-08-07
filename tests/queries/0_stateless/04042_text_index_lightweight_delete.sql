-- Reproduces a bug where MergeTreeReaderTextIndex::readRows overruns past the
-- final mark when the text index reader is non-first in the reader chain.

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t_text_index_lwd_bug;

CREATE TABLE t_text_index_lwd_bug
(
    id UInt64,
    body String,
    created_at DateTime,
    INDEX fts_body body TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    index_granularity = 8192,
    index_granularity_bytes = '10M';

INSERT INTO t_text_index_lwd_bug
SELECT
    number,
    concat('document ', toString(number), if(number % 100 = 0, ' vector', '')),
    toDateTime('2024-01-01 00:00:00') - toIntervalSecond(number)
FROM numbers(200000);

OPTIMIZE TABLE t_text_index_lwd_bug FINAL;

-- Lightweight delete: creates _row_exists column, makes text index reader non-first.
DELETE FROM t_text_index_lwd_bug WHERE id % 1000 = 999;

-- Full scan that reaches the end of the part.
-- The _row_exists reader is first (canReadIncompleteGranules=true),
-- text index reader is second (canReadIncompleteGranules=false).
-- max_block_size=65505 is not a multiple of index_granularity=8192,
-- so batch boundaries fall mid-mark, triggering the drift.
SELECT count()
FROM t_text_index_lwd_bug
WHERE hasToken(body, 'vector') AND created_at >= (toDateTime('2024-01-01 00:00:00') - toIntervalMonth(1))
SETTINGS
    enable_full_text_index = 1,
    use_skip_indexes = 1,
    use_query_condition_cache = 0,
    query_plan_direct_read_from_text_index = 1,
    use_skip_indexes_on_data_read = 1,
    max_threads = 1,
    max_block_size = 65505;

DROP TABLE t_text_index_lwd_bug;
