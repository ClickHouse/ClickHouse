-- A floating-point time series codec is resolved and applied per substream, so a composite type that
-- mixes floating-point and non-floating-point elements would have the codec applied to the
-- non-floating-point elements too. Such a type is only allowed with `allow_suspicious_codecs`.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS t_mixed_tuple_codec;

CREATE TABLE t_mixed_tuple_codec (x Tuple(Float64, UInt64) CODEC(Chimp)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_mixed_tuple_codec (x Tuple(Float64, UInt64) CODEC(Gorilla)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_mixed_tuple_codec (x Tuple(Float64, UInt64) CODEC(FPC)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_mixed_tuple_codec (x Array(Tuple(Float64, String)) CODEC(Chimp)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_mixed_tuple_codec (x Tuple(Float64, Nullable(UInt32)) CODEC(Chimp)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- An all-floating-point composite stays allowed.
CREATE TABLE t_mixed_tuple_codec (x Tuple(Float64, Nullable(Float32)) CODEC(Chimp)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_mixed_tuple_codec VALUES ((1.5, 2.5)), ((3.5, NULL));
SELECT * FROM t_mixed_tuple_codec ORDER BY x;
DROP TABLE t_mixed_tuple_codec;

-- The mixed type is accepted with `allow_suspicious_codecs`, as documented.
SET allow_suspicious_codecs = 1;
CREATE TABLE t_mixed_tuple_codec (x Tuple(Float64, UInt64) CODEC(Chimp)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_mixed_tuple_codec VALUES ((1.5, 100)), ((2.5, 200));
SELECT * FROM t_mixed_tuple_codec ORDER BY x;
DROP TABLE t_mixed_tuple_codec;
