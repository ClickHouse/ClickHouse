DROP TABLE IF EXISTS t_projection_codec_duplicate_add;

CREATE TABLE t_projection_codec_duplicate_add
(
    x UInt64,
    PROJECTION p (SELECT x ORDER BY x)
)
ENGINE = MergeTree ORDER BY x;

-- The existing-name error takes precedence over validation of a declaration that cannot be added.
ALTER TABLE t_projection_codec_duplicate_add
    ADD PROJECTION p (x CODEC(Gorilla)) AS (SELECT x ORDER BY x); -- { serverError ILLEGAL_PROJECTION }
ALTER TABLE t_projection_codec_duplicate_add
    ADD PROJECTION p (missing UInt64 CODEC(NONE)) AS (SELECT missing); -- { serverError ILLEGAL_PROJECTION }

-- Names added earlier in one ALTER reserve the name even if the statement ultimately fails.
ALTER TABLE t_projection_codec_duplicate_add
    ADD PROJECTION q (SELECT x ORDER BY x),
    ADD PROJECTION q (missing UInt64 CODEC(NONE)) AS (SELECT missing); -- { serverError ILLEGAL_PROJECTION }

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_codec_duplicate_add';

-- Dropping the old name earlier in the statement makes the new declaration eligible for validation.
ALTER TABLE t_projection_codec_duplicate_add
    DROP PROJECTION p,
    ADD PROJECTION p (x CODEC(Gorilla)) AS (SELECT x ORDER BY x); -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_codec_duplicate_add';

DROP TABLE t_projection_codec_duplicate_add;
