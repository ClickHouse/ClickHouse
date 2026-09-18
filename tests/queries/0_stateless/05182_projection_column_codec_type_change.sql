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

-- Test type change with data already on disk. The projection part must be rebuilt with codec
-- arguments re-inferred for the new type (e.g., Delta(4) instead of Delta(8) for UInt32).
DROP TABLE IF EXISTS t_projection_untyped_delta_parts;

CREATE TABLE t_projection_untyped_delta_parts
(
    k UInt64,
    d UInt64,
    PROJECTION p (d CODEC(Delta, ZSTD(1))) AS (SELECT k, d ORDER BY k)
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_projection_untyped_delta_parts SELECT number, number FROM numbers(1000);

SELECT 'projection part type before the change';
SELECT column, type FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_untyped_delta_parts' AND active
    AND name = 'p' AND column = 'd';

ALTER TABLE t_projection_untyped_delta_parts MODIFY COLUMN d UInt32 SETTINGS mutations_sync = 2;

SELECT 'declared codec after the change';
SELECT codecs FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_untyped_delta_parts';

SELECT 'projection part type after the change';
SELECT column, type FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_untyped_delta_parts' AND active
    AND name = 'p' AND column = 'd';

-- Reading it back through the projection decompresses what the rebuild wrote.
SELECT 'read from the projection';
SELECT sum(d) FROM t_projection_untyped_delta_parts
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p';

DROP TABLE t_projection_untyped_delta_parts;
