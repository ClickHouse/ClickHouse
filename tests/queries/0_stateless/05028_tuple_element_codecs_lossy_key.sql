-- Tags: no-fasttest
-- no-fasttest: needs the SZ3 library.

DROP TABLE IF EXISTS t_tuple_codec_lossy_sorting_key;
DROP TABLE IF EXISTS t_tuple_codec_lossy_partition_key;
DROP TABLE IF EXISTS t_tuple_codec_lossy_non_key_sibling;

SET allow_experimental_codecs = 1;
SET enable_tuple_element_codecs = 1;

-- A lossy codec on the selected Tuple element would make the stored key value
-- disagree with primary/min-max metadata computed before compression.
CREATE TABLE t_tuple_codec_lossy_sorting_key
(
    x Tuple(k Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), value UInt64)
)
ENGINE = MergeTree
ORDER BY x.k; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_tuple_codec_lossy_partition_key
(
    id UInt64,
    x Tuple(k Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)), value UInt64)
)
ENGINE = MergeTree
PARTITION BY intDiv(toInt64(x.k), 100)
ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- Only streams backing the key element are checked. A lossy codec on an
-- unrelated sibling remains valid.
CREATE TABLE t_tuple_codec_lossy_non_key_sibling
(
    x Tuple(k Float64, value Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)))
)
ENGINE = MergeTree
ORDER BY x.k;

DROP TABLE t_tuple_codec_lossy_non_key_sibling;
