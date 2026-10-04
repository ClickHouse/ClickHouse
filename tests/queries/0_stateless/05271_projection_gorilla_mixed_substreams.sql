SET allow_projection_column_list_in_replicated_metadata = 1;
SET distributed_ddl_output_mode = 'none';
SET database_replicated_always_detach_permanently = 1;

DROP TABLE IF EXISTS t_projection_gorilla_substreams;
DROP TABLE IF EXISTS t_projection_gorilla_substreams_copy;

-- A floating-point time series codec must suit every compressed substream.
CREATE TABLE t_projection_gorilla_substreams
(
    k UInt64,
    x Tuple(Float64, UInt64),
    PROJECTION p (x CODEC(Gorilla)) AS (SELECT k, x ORDER BY k)
)
ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_gorilla_substreams';

CREATE TABLE t_projection_gorilla_substreams
(k UInt64, x Tuple(Float64, UInt64)) ENGINE = MergeTree ORDER BY k;

ALTER TABLE t_projection_gorilla_substreams ADD PROJECTION p
(x CODEC(Gorilla)) AS (SELECT k, x ORDER BY k); -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_gorilla_substreams';

DROP TABLE t_projection_gorilla_substreams;

-- Array sizes and nullable maps are not fed to the special codec.
CREATE TABLE t_projection_gorilla_substreams
(
    k UInt64,
    x Tuple(Array(Float64), Nullable(Float32)),
    PROJECTION p (x CODEC(Gorilla)) AS (SELECT k, x ORDER BY k)
)
ENGINE = MergeTree ORDER BY k;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_gorilla_substreams';

DROP TABLE t_projection_gorilla_substreams;

-- The same factory rule applies to ordinary columns.
CREATE TABLE t_projection_gorilla_substreams
(k UInt64, x Tuple(Float64, UInt64) CODEC(Gorilla))
ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_projection_gorilla_substreams
(
    k UInt64,
    x Array(Tuple(Float64, UInt64)),
    PROJECTION p (x CODEC(Gorilla)) AS (SELECT k, x ORDER BY k)
)
ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }

-- A stored declaration remains loadable, while a new copy applies the current
-- session's codec admission policy.
SET allow_suspicious_codecs = 1;

CREATE TABLE t_projection_gorilla_substreams
(
    k UInt64,
    x Tuple(Float64, UInt64),
    PROJECTION p (x CODEC(Gorilla)) AS (SELECT k, x ORDER BY k)
)
ENGINE = MergeTree ORDER BY k;

SET allow_suspicious_codecs = 0;

CREATE TABLE t_projection_gorilla_substreams_copy AS t_projection_gorilla_substreams
ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_gorilla_substreams_copy';

SET allow_suspicious_codecs = 1;
CREATE TABLE t_projection_gorilla_substreams_copy AS t_projection_gorilla_substreams
ENGINE = MergeTree ORDER BY k;

SET allow_suspicious_codecs = 0;

SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_gorilla_substreams_copy';

DETACH TABLE t_projection_gorilla_substreams;
ATTACH TABLE t_projection_gorilla_substreams;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_gorilla_substreams';

DROP TABLE t_projection_gorilla_substreams;
DROP TABLE t_projection_gorilla_substreams_copy;

-- JSON and Dynamic have structure streams without a data type. Generic codecs
-- must still work, while those streams cannot justify a float-only codec.
CREATE TABLE t_projection_gorilla_substreams
(k UInt64, j JSON(a Float64) CODEC(NONE), d Dynamic CODEC(NONE))
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_projection_gorilla_substreams (k, j, d) SELECT 1, '{"a":1.5}', 7;
SELECT count() FROM t_projection_gorilla_substreams;

DETACH TABLE t_projection_gorilla_substreams;
ATTACH TABLE t_projection_gorilla_substreams;
SELECT count() FROM t_projection_gorilla_substreams;

DROP TABLE t_projection_gorilla_substreams;

CREATE TABLE t_projection_gorilla_substreams
(k UInt64, j JSON(a Float64) CODEC(Gorilla))
ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_gorilla_substreams';
