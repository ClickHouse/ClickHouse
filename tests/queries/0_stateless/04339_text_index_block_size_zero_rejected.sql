-- Tests that the text index block size settings reject 0, both at CREATE and via MODIFY SETTING.
-- A zero block size previously reached the index build and caused an integer divide by zero.

DROP TABLE IF EXISTS t_block_size_zero;

-- CREATE with a zero block size setting is rejected.
CREATE TABLE t_block_size_zero (s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS text_index_dictionary_block_size = 0; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_block_size_zero (s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS text_index_posting_list_block_size = 0; -- { serverError BAD_ARGUMENTS }

-- A valid table; MODIFY SETTING to zero must be rejected so it cannot reach an insert.
CREATE TABLE t_block_size_zero (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple();

ALTER TABLE t_block_size_zero MODIFY SETTING text_index_dictionary_block_size = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_block_size_zero MODIFY SETTING text_index_posting_list_block_size = 0; -- { serverError BAD_ARGUMENTS }

-- The table is still usable with the (unchanged, valid) settings.
INSERT INTO t_block_size_zero SELECT 'hello world ' || toString(number) FROM numbers(100);
SELECT count() FROM t_block_size_zero WHERE hasToken(s, 'hello');

DROP TABLE t_block_size_zero;
