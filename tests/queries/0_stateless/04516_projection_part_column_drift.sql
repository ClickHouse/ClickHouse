-- Projection metadata is re-derived from the projection query at every table load, so an existing
-- projection part may lack a column the current metadata expects (e.g. after ALTER re-points an
-- ALIAS column selected by the projection). Reading or merging such a part must not fill the
-- missing column with defaults: reads fall back to the parent part, merges rebuild the projection.

-- The projections here select an ALIAS column, so building them on INSERT requires the alias to be
-- resolved. Under optimize_respect_aliases=0 that resolution is skipped and every INSERT fails with
-- UNKNOWN_IDENTIFIER, independently of the drift this test exercises (reproduces on master too), so
-- pin the setting away from the randomized value.
-- Random settings limits: optimize_respect_aliases=(1, 1)

DROP TABLE IF EXISTS t_proj_column_drift;

CREATE TABLE t_proj_column_drift
(
    a UInt64,
    b UInt64,
    d UInt64,
    c UInt64 ALIAS b + 1,
    PROJECTION p (SELECT a, c ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_column_drift (a, b, d) VALUES (1, 100, 500);

-- the projection part stores the alias source `b`
SELECT 'part columns before drift', name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_proj_column_drift' AND active ORDER BY name, column;

-- re-point the alias: the part still stores {a, b}, but the re-derived metadata now expects {a, d}
ALTER TABLE t_proj_column_drift MODIFY COLUMN c UInt64 ALIAS d + 1;

-- the drifted part must be served from the parent (c = 501, not 1 from a default-filled d)
SELECT 'read after drift', a, c FROM t_proj_column_drift ORDER BY a;

-- forcing the projection cannot use the drifted part
SELECT a, c FROM t_proj_column_drift ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

-- a part written after the alter is not drifted; the mixed read stays correct
INSERT INTO t_proj_column_drift (a, b, d) VALUES (2, 200, 600);

SELECT 'mixed read', a, c FROM t_proj_column_drift ORDER BY a;

-- merging a drifted part must rebuild the projection from the parent data instead of
-- baking default values for the missing column into the merged projection part
OPTIMIZE TABLE t_proj_column_drift FINAL;

SELECT 'read after merge', a, c FROM t_proj_column_drift ORDER BY a;

SELECT a, c FROM t_proj_column_drift ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT 'part columns after merge', name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_proj_column_drift' AND active ORDER BY name, column;

DROP TABLE t_proj_column_drift;

-- must-not-act control (the #108569 scenario): a column added to the table after the part was
-- written is missing from BOTH the projection part and the parent part; the default fill is then
-- correct and the projection must still be usable
DROP TABLE IF EXISTS t_proj_added_column;

CREATE TABLE t_proj_added_column
(
    a UInt64,
    PROJECTION p (SELECT * ORDER BY a)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_added_column VALUES (1);

ALTER TABLE t_proj_added_column ADD COLUMN e UInt64 DEFAULT 42;

SELECT 'added column via projection', a, e FROM t_proj_added_column ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_proj_added_column;

-- aggregate projection drift: the state column name embeds the expanded alias
-- (`sum(plus(b, 1))`), so re-pointing the alias leaves the part without the state column the
-- metadata now expects (`sum(plus(d, 1))`); the parent never stores aggregate states, so the
-- absence must still count as drift
DROP TABLE IF EXISTS t_proj_agg_drift;

CREATE TABLE t_proj_agg_drift
(
    a UInt64,
    b UInt64,
    d UInt64,
    c UInt64 ALIAS b + 1,
    PROJECTION p (SELECT a, sum(c) GROUP BY a)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_agg_drift (a, b, d) VALUES (1, 100, 500);

ALTER TABLE t_proj_agg_drift MODIFY COLUMN c UInt64 ALIAS d + 1;

SELECT 'agg read after drift', a, sum(c) FROM t_proj_agg_drift GROUP BY a ORDER BY a;

SELECT a, sum(c) FROM t_proj_agg_drift GROUP BY a ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

INSERT INTO t_proj_agg_drift (a, b, d) VALUES (2, 200, 600);

OPTIMIZE TABLE t_proj_agg_drift FINAL;

SELECT 'agg read after merge', a, sum(c) FROM t_proj_agg_drift GROUP BY a ORDER BY a;

SELECT a, sum(c) FROM t_proj_agg_drift GROUP BY a ORDER BY a
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_proj_agg_drift;

-- virtual-column control: virtuals are provided by the reading step, not stored in the part;
-- requiring one must not disqualify the projection
DROP TABLE IF EXISTS t_proj_virtual;

CREATE TABLE t_proj_virtual
(
    a UInt64,
    b UInt64,
    PROJECTION p (SELECT a, b ORDER BY b)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_proj_virtual VALUES (1, 100);

SELECT 'virtual via projection', a, b, _part != '' FROM t_proj_virtual WHERE b = 100
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_proj_virtual;
