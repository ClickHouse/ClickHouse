-- Regression test for the Chimp codec in a `TTL ... RECOMPRESS CODEC(...)` clause.
--
-- A bare `Chimp` (no explicit width argument, no column type) is built with an undetermined width
-- and cannot compress; it stays constructible only for method-byte decoding and `system.codecs`.
-- The TTL RECOMPRESS validation path passes no column type, so it used to accept bare `Chimp`,
-- persist `CODEC(Chimp)`, and fail only later during the background recompression merge. It must
-- instead be rejected up front at DDL time with `ILLEGAL_CODEC_PARAMETER`.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS t_chimp_recompress;
DROP TABLE IF EXISTS t_chimp_column;

-- Bare `Chimp` in a `TTL ... RECOMPRESS` clause has no column type to derive a width from, so it
-- is rejected during DDL validation. Note this holds regardless of `allow_experimental_codecs`,
-- because the undetermined-width check runs before the experimental-codec check.
CREATE TABLE t_chimp_recompress
(
    dt DateTime,
    x Float64
)
ENGINE = MergeTree ORDER BY tuple()
TTL dt + INTERVAL 100 YEAR RECOMPRESS CODEC(Chimp); -- { serverError ILLEGAL_CODEC_PARAMETER }

-- With a column type the same bare `Chimp` resolves its width (8 bytes for `Float64`) and is a
-- usable write codec, so the rejection above is specific to the width-undetermined case and not to
-- `Chimp` in general. (An experimental codec cannot be used in `RECOMPRESS` regardless of width,
-- because that validation runs against the global context, which does not see the session-level
-- `allow_experimental_codecs` setting - the same as for the existing `ALP` codec.)
CREATE TABLE t_chimp_column
(
    dt DateTime,
    x Float64 CODEC(Chimp)
)
ENGINE = MergeTree ORDER BY tuple();

SELECT 'created';

DROP TABLE t_chimp_column;
