-- A text index block size close to the maximum of UInt64 must still return the rows that were
-- written. The zero end of the same domain is covered by 04339_text_index_block_size_zero_rejected.

DROP TABLE IF EXISTS t_text_block_size_near_max;

-- A text index is built for the whole part and ignores GRANULARITY, so the rounding only matters
-- when the part holds at least two distinct tokens; every row here carries two.
CREATE TABLE t_text_block_size_near_max (k UInt64, s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', dictionary_block_size = 18446744073709551615))
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_text_block_size_near_max VALUES (1, 'alpha beta'), (2, 'gamma delta');

SELECT 'near max, rows', count() FROM t_text_block_size_near_max
SETTINGS optimize_trivial_count_query = 0;
SELECT 'near max, token', count() FROM t_text_block_size_near_max WHERE hasToken(s, 'alpha')
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_text_block_size_near_max;

DROP TABLE IF EXISTS t_text_block_size_small;

-- The same data and query at a small block size, with the index required to be used.
CREATE TABLE t_text_block_size_small (k UInt64, s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', dictionary_block_size = 4))
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_text_block_size_small VALUES (1, 'alpha beta'), (2, 'gamma delta');

SELECT 'small, token', count() FROM t_text_block_size_small WHERE hasToken(s, 'alpha')
SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_text_block_size_small;
