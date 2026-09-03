DROP TABLE IF EXISTS tbl;

-- Checks that (floating-point) time series codecs can be combined
--   with Nullable and
--   with composite types Array and Tuple

CREATE TABLE tbl (
    -- Nullable
    v1_gor Nullable(Float64) CODEC(Gorilla),
    v1_fpc Nullable(Float64) CODEC(FPC),
    -- Array
    v2_gor Array(Float64) CODEC(Gorilla),
    v2_fpc Array(Float64) CODEC(FPC),
    v3_gor Array(Array(Float64)) CODEC(Gorilla),
    v3_fpc Array(Array(Float64)) CODEC(FPC),
    v4_gor Array(Nullable(Float64)) CODEC(Gorilla),
    v4_fpc Array(Nullable(Float64)) CODEC(FPC),
    v5_gor Array(Tuple(Float64)) CODEC(Gorilla),
    v5_fpc Array(Tuple(Float64)) CODEC(FPC),
    -- Tuple
    v6_gor Tuple(Float64) CODEC(Gorilla),
    v6_fpc Tuple(Float64) CODEC(FPC),
    v7_gor Tuple(Tuple(Float64)) CODEC(Gorilla),
    v7_fpc Tuple(Tuple(Float64)) CODEC(FPC),
    v8_gor Tuple(Nullable(Float64)) CODEC(Gorilla),
    v8_fpc Tuple(Nullable(Float64)) CODEC(FPC),
    v9_gor Tuple(Array(Float64)) CODEC(Gorilla),
    v9_fpc Tuple(Array(Float64)) CODEC(FPC),
) Engine = MergeTree ORDER BY tuple();

DROP TABLE IF EXISTS tbl;
