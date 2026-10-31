SET allow_experimental_projection_text_index = 1;
-- Regression test for partial-mark reads through `MergeTreeReaderTextProjectionIndex`.
--
-- `MergeTreeReaderTextProjectionIndex::readRows` interprets `rows_offset` as a
-- jump inside the current mark, and forward-only cursors must produce postings
-- aligned with `[from_row, from_row + rows_to_read)`. When the read window
-- starts or ends inside a mark, an off-by-one in posting alignment could
-- produce out-of-bounds writes into the resized column buffer.
--
-- Forcing `max_block_size` smaller than the index granularity makes the
-- range reader split each granule into many calls to `readRows`, so almost
-- every block start sits inside a mark and exercises the partial-mark path.

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t_projection_text_index_partial_mark;

CREATE TABLE t_projection_text_index_partial_mark
(
    id UInt64,
    body String,
    PROJECTION idx INDEX body TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- ~50 marks of 8192 rows. Token `needle` is present in every 37th row so it
-- is scattered across blocks within every mark, and reads that start or end
-- mid-mark must still resolve matching postings to in-bounds offsets.
INSERT INTO t_projection_text_index_partial_mark
SELECT number, concat('row_', toString(number), if(number % 37 = 0, ' needle', ''))
FROM numbers(400000);

SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

-- Result must match a brute-force scan that doesn't use the index.
SELECT count() FROM t_projection_text_index_partial_mark WHERE hasToken(body, 'needle') SETTINGS use_skip_indexes = 1, max_threads = 1, max_block_size = 100;
SELECT count() FROM t_projection_text_index_partial_mark WHERE hasToken(body, 'needle') SETTINGS use_skip_indexes = 0;

DROP TABLE t_projection_text_index_partial_mark;
