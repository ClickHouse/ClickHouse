-- Regression test for the Chimp codec in a `TTL ... RECOMPRESS CODEC(...)` clause.
--
-- A bare `Chimp` (no explicit width argument, no column type) is built with an undetermined width
-- and cannot compress; it stays constructible only for method-byte decoding and `system.codecs`.
-- The TTL RECOMPRESS validation path passes no column type, so it used to accept bare `Chimp`,
-- persist `CODEC(Chimp)`, and fail only later during the background recompression merge. It must
-- instead be rejected up front at DDL time with `ILLEGAL_CODEC_PARAMETER`.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS t_chimp_recompress;

-- Bare `Chimp` has no column type to derive a width from: rejected during DDL validation.
CREATE TABLE t_chimp_recompress
(
    dt DateTime,
    x Float64
)
ENGINE = MergeTree ORDER BY tuple()
TTL dt + INTERVAL 100 YEAR RECOMPRESS CODEC(Chimp); -- { serverError ILLEGAL_CODEC_PARAMETER }

-- An explicit width gives a usable codec, so DDL validation passes.
CREATE TABLE t_chimp_recompress
(
    dt DateTime,
    x Float64
)
ENGINE = MergeTree ORDER BY tuple()
TTL dt + INTERVAL 100 YEAR RECOMPRESS CODEC(Chimp(8));

SELECT 'created';

DROP TABLE t_chimp_recompress;
