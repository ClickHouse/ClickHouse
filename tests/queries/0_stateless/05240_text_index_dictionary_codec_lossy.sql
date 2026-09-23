-- Tags: no-fasttest
-- no-fasttest: the SZ3 codec needs the sz3 library

-- A lossy codec is rejected in both forms of the text index dictionary codec even when its own gate
-- is open: the dictionary is compressed without a column data type, so the codec is built with a
-- null type and `CompressionCodecFactory::get` refuses a lossy codec in that context. SZ3 is the
-- only lossy codec, which is the dependency that keeps these two rows out of
-- 04616_experimental_codec_in_merge_tree_settings, which must stay fasttest-eligible.

DROP TABLE IF EXISTS t_lossy;

SET enable_sz3_codec = 1;

SELECT 'table setting';
CREATE TABLE t_lossy (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS text_index_dictionary_compression_codec = 'SZ3'; -- { serverError BAD_ARGUMENTS }

SELECT 'index argument';
CREATE TABLE t_lossy (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', dictionary_compression_codec = 'SZ3')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

SET enable_sz3_codec = 0;
