-- { echo ON }

DROP TABLE IF EXISTS t_agg_codecs;

-- An aggregate projection names its columns after the expression text, so a declaration uses
-- `max(v)`, not `v`. The type being optional, the aggregate state type need not be spelled out.
CREATE TABLE t_agg_codecs
(
    id UInt64,
    v UInt64,
    PROJECTION p
    (
        `max(v)` CODEC(ZSTD(3))
    )
    AS
    (
        SELECT id, max(v) GROUP BY id
    )
)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_agg_codecs SELECT number % 50000, number FROM numbers(200000);

SHOW CREATE TABLE t_agg_codecs;

SELECT name, codecs FROM system.projections
WHERE database = currentDatabase() AND table = 't_agg_codecs';

-- Exercise the aggregate projection's stored state. A generic compressed-size comparison would pass
-- with the default codec too and would not prove anything about this declaration.
SELECT id, max(v) FROM t_agg_codecs GROUP BY id ORDER BY id LIMIT 3
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p';

-- The declaration survives a round trip.
DETACH TABLE t_agg_codecs;
ATTACH TABLE t_agg_codecs;
SHOW CREATE TABLE t_agg_codecs;

DROP TABLE t_agg_codecs;

-- Spelling the aggregate state type out explicitly is accepted too.
CREATE TABLE t_agg_typed
(
    id UInt64,
    v UInt64,
    PROJECTION p
    (
        `max(v)` AggregateFunction(max, UInt64) CODEC(ZSTD(3))
    )
    AS
    (
        SELECT id, max(v) GROUP BY id
    )
)
ENGINE = MergeTree ORDER BY id;

SHOW CREATE TABLE t_agg_typed;
DROP TABLE t_agg_typed;

-- The underlying type is a mismatch, not a silent reinterpretation.
CREATE TABLE t_agg_bad (id UInt64, v UInt64, PROJECTION p (`max(v)` UInt64 CODEC(ZSTD(3))) AS (SELECT id, max(v) GROUP BY id))
ENGINE = MergeTree ORDER BY id; -- { serverError TYPE_MISMATCH }

-- The underlying column name is not produced by the projection at all.
CREATE TABLE t_agg_bad (id UInt64, v UInt64, PROJECTION p (v CODEC(ZSTD(3))) AS (SELECT id, max(v) GROUP BY id))
ENGINE = MergeTree ORDER BY id; -- { serverError THERE_IS_NO_COLUMN }
