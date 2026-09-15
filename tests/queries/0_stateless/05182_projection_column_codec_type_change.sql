DROP TABLE IF EXISTS t_projection_untyped_delta;

-- An untyped declaration must infer type-dependent codec arguments again when the projection's
-- output type changes. Persisting `Delta(8)` from the original `UInt64` would pin old type information.
CREATE TABLE t_projection_untyped_delta
(
    k UInt64,
    d UInt64,
    PROJECTION p (d CODEC(Delta, ZSTD(1))) AS (SELECT k, d ORDER BY k)
)
ENGINE = MergeTree ORDER BY k;

SELECT codecs FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_untyped_delta';

ALTER TABLE t_projection_untyped_delta MODIFY COLUMN d UInt32;

SELECT codecs FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_untyped_delta';

INSERT INTO t_projection_untyped_delta VALUES (1, 1);
SELECT d FROM t_projection_untyped_delta
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p';

DROP TABLE t_projection_untyped_delta;
