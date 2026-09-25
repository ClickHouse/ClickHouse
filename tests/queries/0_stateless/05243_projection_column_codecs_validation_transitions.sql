DROP TABLE IF EXISTS t_projection_codec_attach;
DROP TABLE IF EXISTS t_projection_codec_type_change;

-- A full-definition `ATTACH` is fresh user input, so its projection codecs are checked against
-- the session settings just like `CREATE`.
ATTACH TABLE t_projection_codec_attach UUID '00000000-0000-0000-0000-000000005243'
(
    k UInt64,
    x UInt64,
    PROJECTION p (x CODEC(Gorilla)) AS (SELECT k, x ORDER BY k)
)
ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }

SET allow_suspicious_codecs = 1;
ATTACH TABLE t_projection_codec_attach UUID '00000000-0000-0000-0000-000000005243'
(
    k UInt64,
    x UInt64,
    PROJECTION p (x CODEC(Gorilla)) AS (SELECT k, x ORDER BY k)
)
ENGINE = MergeTree ORDER BY k;
SET allow_suspicious_codecs = 0;

-- Loading the stored definition remains a trusted path after the opt-in setting is reset.
DETACH TABLE t_projection_codec_attach;
ATTACH TABLE t_projection_codec_attach;

SELECT 'attached', codecs FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_codec_attach';

DROP TABLE t_projection_codec_attach;

-- An untyped declaration follows the `SELECT` output type. Changing that type must re-run the
-- session-gated codec check, while a change to an unrelated column must leave the accepted codec alone.
CREATE TABLE t_projection_codec_type_change
(
    k UInt64,
    x Float64,
    y UInt8,
    PROJECTION p (x CODEC(Gorilla)) AS (SELECT k, x ORDER BY k)
)
ENGINE = MergeTree ORDER BY k;

ALTER TABLE t_projection_codec_type_change MODIFY COLUMN x UInt64; -- { serverError BAD_ARGUMENTS }

-- Compare against the pre-`ALTER` description even if a later command rebuilds the projection first.
ALTER TABLE t_projection_codec_type_change
    MODIFY COLUMN x UInt64,
    MODIFY PROJECTION p (x CODEC(Gorilla)) AS (SELECT k, x ORDER BY k)
        WITH SETTINGS (index_granularity = 128); -- { serverError BAD_ARGUMENTS }

SET allow_suspicious_codecs = 1;
ALTER TABLE t_projection_codec_type_change MODIFY COLUMN x UInt64;
SET allow_suspicious_codecs = 0;

ALTER TABLE t_projection_codec_type_change MODIFY COLUMN y UInt16;

SELECT 'altered', codecs FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_codec_type_change';

DROP TABLE t_projection_codec_type_change;
