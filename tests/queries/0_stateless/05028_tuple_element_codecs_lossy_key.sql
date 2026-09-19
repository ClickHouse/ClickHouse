-- Tags: no-fasttest
-- no-fasttest: needs the SZ3 library.

DROP TABLE IF EXISTS t_tuple_codec_lossy_sorting_key;
DROP TABLE IF EXISTS t_tuple_codec_lossy_partition_key;
DROP TABLE IF EXISTS t_tuple_codec_lossy_non_key_sibling;
DROP TABLE IF EXISTS t_tuple_codec_lossy_map_element;
DROP TABLE IF EXISTS t_tuple_codec_lossy_colliding_subcolumn;

SET enable_sz3_codec = 1;
SET enable_tuple_element_codecs = 1;

-- Per-stream validation must retain the logical route into Map. Its Float64 leaves
-- are valid SZ3 inputs individually, but lossy compression could change Map keys.
CREATE TABLE t_tuple_codec_lossy_map_element
(
    x Tuple(
        m Map(Float64, Float64)
            CODEC(SZ3('ALGO_INTERP', 'REL', 0.01))
    )
)
ENGINE = MergeTree
ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

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

-- Exact subcolumn names use the first match. Here x.a.b names the literal
-- Tuple element, not the later nested path with the same flattened name.
CREATE TABLE t_tuple_codec_lossy_colliding_subcolumn
(
    x Tuple(
        `a.b` Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)),
        a Tuple(b Float64 CODEC(LZ4))
    )
)
ENGINE = MergeTree
ORDER BY x.a.b; -- { serverError BAD_ARGUMENTS }

-- Only streams backing the key element are checked. A lossy codec on an
-- unrelated sibling remains valid.
CREATE TABLE t_tuple_codec_lossy_non_key_sibling
(
    x Tuple(k Float64, value Float64 CODEC(SZ3('ALGO_INTERP_LORENZO', 'REL', 0.01)))
)
ENGINE = MergeTree
ORDER BY x.k;

DROP TABLE t_tuple_codec_lossy_non_key_sibling;
