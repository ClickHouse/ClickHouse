-- Tags: no-fasttest
-- no-fasttest: the lossy-codec validation uses SZ3, which is not registered in builds without the sz3 library.

-- { echo ON }

DROP TABLE IF EXISTS t_codec_validation;

-- Whether a declaration spells out the type must not change which codecs are accepted.

-- A floating-point time series codec on an integer column is suspicious, declared type or not.
CREATE TABLE t_codec_validation
(
    x UInt64,
    PROJECTION p
    (
        x UInt64 CODEC(Gorilla)
    )
    AS
    (
        SELECT x ORDER BY x
    )
)
ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_codec_validation
(
    x UInt64,
    PROJECTION p
    (
        x CODEC(Gorilla)
    )
    AS
    (
        SELECT x ORDER BY x
    )
)
ENGINE = MergeTree ORDER BY x; -- { serverError BAD_ARGUMENTS }

-- The setting still reaches the check, so the user can opt out of it.
SET allow_suspicious_codecs = 1;

CREATE TABLE t_codec_validation
(
    x UInt64,
    PROJECTION p
    (
        x CODEC(Gorilla)
    )
    AS
    (
        SELECT x ORDER BY x
    )
)
ENGINE = MergeTree ORDER BY x;

SELECT codecs FROM system.projections WHERE database = currentDatabase() AND table = 't_codec_validation';

DROP TABLE t_codec_validation;
SET allow_suspicious_codecs = 0;

-- A projection must return the same values as its parent table, so a lossy codec is rejected even
-- when the codec is enabled and the encoded column is not part of the projection's sorting key.
SET enable_sz3_codec = 1;

CREATE TABLE t_codec_validation
(
    id UInt64,
    x Float64,
    PROJECTION p
    (
        x CODEC(SZ3)
    )
    AS
    (
        SELECT id, x ORDER BY id
    )
)
ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

SET enable_sz3_codec = 0;

-- The same check applies when the projection arrives by `ALTER`.
CREATE TABLE t_codec_validation (x UInt64) ENGINE = MergeTree ORDER BY x;

ALTER TABLE t_codec_validation ADD PROJECTION p
(
    x CODEC(Gorilla)
)
AS
(
    SELECT x ORDER BY x
); -- { serverError BAD_ARGUMENTS }

SET allow_suspicious_codecs = 1;

ALTER TABLE t_codec_validation ADD PROJECTION p
(
    x CODEC(Gorilla)
)
AS
(
    SELECT x ORDER BY x
);

SELECT codecs FROM system.projections WHERE database = currentDatabase() AND table = 't_codec_validation';

SET allow_suspicious_codecs = 0;
DROP TABLE t_codec_validation;

-- A keyword is usable as a declared column name when quoted; the declaration list is only
-- distinguished from a `SELECT` by the leading token.
CREATE TABLE t_codec_validation
(
    `select` UInt64,
    PROJECTION p
    (
        `select` CODEC(ZSTD(3))
    )
    AS
    (
        SELECT `select` ORDER BY `select`
    )
)
ENGINE = MergeTree ORDER BY `select`;

SELECT codecs FROM system.projections WHERE database = currentDatabase() AND table = 't_codec_validation';

DROP TABLE t_codec_validation;
